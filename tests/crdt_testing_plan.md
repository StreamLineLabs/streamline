# Jepsen-Class CRDT Correctness Testing Plan

> **Scope:** Streamline's native CRDT subsystem — GCounter, PNCounter, LWWRegister, ORSet, RGASequence — under adversarial network conditions, extended offline periods, and concurrent multi-node writes.
>
> **Goal:** Prove that every CRDT type satisfies its mathematical contract (commutativity, idempotency, associativity, monotonicity) and that the system achieves **strong eventual consistency** across edge nodes, cloud replicas, and geo-distributed regions even in the presence of partitions, clock drift, and week-long disconnections.

---

## Table of Contents

1. [Property-Based Tests for Each CRDT Type](#1-property-based-tests-for-each-crdt-type)
   - [1.1 LWW Register](#11-lww-register-last-writer-wins)
   - [1.2 GCounter / PNCounter](#12-gcounter--pncounter)
   - [1.3 ORSet](#13-orset-observed-remove-set)
   - [1.4 RGASequence (Multi-Value / Collaborative)](#14-rgasequence-multi-value--collaborative)
   - [1.5 MVRegister (Multi-Value Register)](#15-mvregister-multi-value-register)
2. [Partition Simulation Scenarios](#2-partition-simulation-scenarios)
3. [Convergence Verification Approach](#3-convergence-verification-approach)
4. [7-Day Offline Reconnect Test Plan](#4-7-day-offline-reconnect-test-plan)
5. [Test Infrastructure & Tooling](#5-test-infrastructure--tooling)
6. [Acceptance Criteria](#6-acceptance-criteria)

---

## 1. Property-Based Tests for Each CRDT Type

All property-based tests use randomized inputs via `proptest` (Rust) or equivalent QuickCheck-style frameworks. Each property is tested with ≥10,000 generated cases and additional shrinking on failure.

### 1.1 LWW Register (Last-Writer-Wins)

Streamline implements LWW via `LWWRegister<T>` (in `src/crdt/types.rs`) and the edge-layer `merge_lww` (in `src/edge/crdt.rs`). Conflict resolution: higher HLC timestamp wins; on tie, lexicographically larger `node_id` wins.

#### Properties

| Property | Formal Statement | Description |
|---|---|---|
| **Commutativity** | `merge(a, b) == merge(b, a)` | Merge order must not affect result |
| **Idempotency** | `merge(a, a) == a` | Re-merging the same state is a no-op |
| **Associativity** | `merge(merge(a, b), c) == merge(a, merge(b, c))` | Grouping of merges must not matter |
| **Timestamp Ordering** | If `ts(a) > ts(b)`, then `merge(a, b).value == a.value` | Later timestamp always wins |
| **Tiebreak Determinism** | If `ts(a) == ts(b)`, result is deterministic by node_id | Equal timestamps resolve by node_id lexicographic order |

#### Pseudocode

```rust
#[cfg(test)]
mod lww_property_tests {
    use proptest::prelude::*;
    use crate::crdt::{LWWRegister};

    // Strategy: generate random LWW registers with varying timestamps and node IDs
    fn arb_lww() -> impl Strategy<Value = LWWRegister<String>> {
        (any::<u64>(), "[a-z]{1,8}", "[a-z0-9]{1,32}")
            .prop_map(|(ts, node, val)| {
                LWWRegister::with_timestamp(&node, val, ts as i64)
            })
    }

    proptest! {
        #[test]
        fn lww_commutativity(a in arb_lww(), b in arb_lww()) {
            let mut ab = a.clone();
            ab.merge(&b);
            let mut ba = b.clone();
            ba.merge(&a);
            prop_assert_eq!(ab.value(), ba.value());
        }

        #[test]
        fn lww_idempotency(a in arb_lww()) {
            let mut aa = a.clone();
            aa.merge(&a);
            prop_assert_eq!(aa.value(), a.value());
        }

        #[test]
        fn lww_associativity(a in arb_lww(), b in arb_lww(), c in arb_lww()) {
            let mut ab_c = a.clone();
            ab_c.merge(&b);
            ab_c.merge(&c);

            let mut a_bc = a.clone();
            let mut bc = b.clone();
            bc.merge(&c);
            a_bc.merge(&bc);

            prop_assert_eq!(ab_c.value(), a_bc.value());
        }

        #[test]
        fn lww_timestamp_ordering(
            node1 in "[a-z]{1,4}",
            node2 in "[a-z]{1,4}",
            val1 in "[a-z]{1,8}",
            val2 in "[a-z]{1,8}",
            ts_early in 0u64..1_000_000,
            ts_delta in 1u64..1_000_000,
        ) {
            let ts_late = ts_early + ts_delta;
            let early = LWWRegister::with_timestamp(&node1, val1, ts_early as i64);
            let late = LWWRegister::with_timestamp(&node2, val2.clone(), ts_late as i64);

            let mut merged = early.clone();
            merged.merge(&late);
            prop_assert_eq!(merged.value(), &val2);
        }

        #[test]
        fn lww_tiebreak_determinism(
            node1 in "[a-z]{1,8}",
            node2 in "[a-z]{1,8}",
            val1 in "[a-z]{1,8}",
            val2 in "[a-z]{1,8}",
            ts in 0u64..1_000_000,
        ) {
            let a = LWWRegister::with_timestamp(&node1, val1, ts as i64);
            let b = LWWRegister::with_timestamp(&node2, val2, ts as i64);

            let mut m1 = a.clone();
            m1.merge(&b);
            let mut m2 = b.clone();
            m2.merge(&a);

            // Same result regardless of merge order
            prop_assert_eq!(m1.value(), m2.value());

            // Winner is the one with the lexicographically larger node_id
            let expected_winner = if node1 >= node2 { &node1 } else { &node2 };
            // The winning value corresponds to the winning node
            // (verified by checking merge result is stable)
        }
    }
}
```

### 1.2 GCounter / PNCounter

`GCounter` is a grow-only counter using per-node counters merged with max. `PNCounter` wraps two `GCounter`s (increments, decrements).

#### GCounter Properties

| Property | Formal Statement |
|---|---|
| **Commutativity** | `merge(a, b) == merge(b, a)` |
| **Idempotency** | `merge(a, a) == a` |
| **Associativity** | `merge(merge(a, b), c) == merge(a, merge(b, c))` |
| **Monotonicity** | `a.value() <= merge(a, b).value()` for all `b` |
| **Union Correctness** | `merge(a, b).value() == sum(max(a[node], b[node]) for each node)` |

#### GSet Properties (GCounter's set analog)

Since Streamline models the GSet concept through `ORSet` without removal, we test the GSet invariant as an ORSet subcase:

| Property | Description |
|---|---|
| **Commutativity** | `merge(a, b) == merge(b, a)` (same element set) |
| **Idempotency** | `merge(a, a) == a` (no duplicates introduced) |
| **Monotonicity** | Elements are never removed; `|merge(a, b)| >= max(|a|, |b|)` |
| **Union Correctness** | `elements(merge(a, b)) == elements(a) ∪ elements(b)` |

#### Pseudocode

```rust
proptest! {
    #[test]
    fn gcounter_commutativity(
        nodes in prop::collection::vec("[a-z]{1,4}", 1..5),
        increments in prop::collection::vec(1u64..1000, 1..10),
    ) {
        // Build two counters with different subsets of increments
        let (a, b) = build_counters_from(&nodes, &increments);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        prop_assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn gcounter_monotonicity(
        a in arb_gcounter(),
        b in arb_gcounter(),
    ) {
        let original = a.value();
        let mut merged = a.clone();
        merged.merge(&b);
        prop_assert!(merged.value() >= original);
    }

    #[test]
    fn pncounter_commutativity(a in arb_pncounter(), b in arb_pncounter()) {
        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);
        prop_assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn pncounter_increment_then_decrement(
        node in "[a-z]{1,4}",
        inc in 1u64..1000,
        dec in 1u64..500,
    ) {
        let mut counter = PNCounter::new(&node);
        counter.increment(inc);
        counter.decrement(dec);
        prop_assert_eq!(counter.value(), inc as i64 - dec as i64);
    }
}
```

### 1.3 ORSet (Observed-Remove Set)

`ORSet<T>` uses unique tags (`UniqueTag { node_id, counter }`) per add operation. A remove only removes tags observed at the time of removal — concurrent adds with fresh tags survive.

#### Properties

| Property | Formal Statement | Description |
|---|---|---|
| **Add-Wins Semantics** | Concurrent add + remove → element present | A concurrent add generates a fresh tag unseen by the remover |
| **Commutativity** | `merge(a, b) == merge(b, a)` | Same element set regardless of merge order |
| **Idempotency** | `merge(a, a) == a` | No duplicate tags after self-merge |
| **Tombstone Handling** | Removed tags never reappear | Tombstone set grows monotonically |
| **Concurrent Add/Remove** | Both operations from different nodes resolve correctly | Fresh unique tags survive removal of stale tags |
| **Associativity** | `merge(merge(a, b), c) == merge(a, merge(b, c))` | Grouping doesn't affect result |

#### Pseudocode

```rust
proptest! {
    #[test]
    fn orset_add_wins(
        item in "[a-z]{1,8}",
        node1 in "[a-z]{1,4}",
        node2 in "[a-z]{1,4}",
    ) {
        // Start with item in the set on both replicas
        let mut set1: ORSet<String> = ORSet::new(&node1);
        set1.add(item.clone());

        let mut set2 = set1.clone();
        set2.set_local_node(&node2);

        // Node1 removes, node2 concurrently re-adds
        set1.remove(&item);
        set2.add(item.clone());  // Fresh unique tag

        // Merge: the fresh add tag from node2 survives
        set1.merge(&set2);
        prop_assert!(set1.contains(&item), "Add-wins violated");
    }

    #[test]
    fn orset_commutativity(
        ops_a in arb_orset_ops(5),
        ops_b in arb_orset_ops(5),
    ) {
        let a = apply_ops(ORSet::new("a"), &ops_a);
        let b = apply_ops(ORSet::new("b"), &ops_b);

        let mut ab = a.clone(); ab.merge(&b);
        let mut ba = b.clone(); ba.merge(&a);

        prop_assert_eq!(ab.elements(), ba.elements());
    }

    #[test]
    fn orset_tombstone_permanence(
        item in "[a-z]{1,8}",
        node in "[a-z]{1,4}",
    ) {
        let mut set: ORSet<String> = ORSet::new(&node);
        set.add(item.clone());

        let tag_before_remove = set.tags_for(&item);
        set.remove(&item);

        // Re-add generates a new unique tag
        set.add(item.clone());
        let tag_after_readd = set.tags_for(&item);

        // Original tags must not reappear
        for tag in &tag_before_remove {
            prop_assert!(!tag_after_readd.contains(tag));
        }
    }

    #[test]
    fn orset_concurrent_add_remove_from_diverged_replicas(
        items in prop::collection::hash_set("[a-z]{1,4}", 1..10),
    ) {
        let mut replica_a: ORSet<String> = ORSet::new("node-a");
        let mut replica_b: ORSet<String> = ORSet::new("node-b");

        // Both add all items
        for item in &items {
            replica_a.add(item.clone());
            replica_b.add(item.clone());
        }
        // Sync
        replica_a.merge(&replica_b);
        replica_b.merge(&replica_a);

        // Partition: A removes half, B adds new items concurrently
        let remove_items: Vec<_> = items.iter().take(items.len() / 2).cloned().collect();
        for item in &remove_items {
            replica_a.remove(item);
        }
        for i in 0..3 {
            replica_b.add(format!("new-{}", i));
        }

        // Heal partition
        replica_a.merge(&replica_b);
        replica_b.merge(&replica_a);

        // Both replicas must converge to identical state
        prop_assert_eq!(replica_a.elements(), replica_b.elements());

        // New items from B must be present
        for i in 0..3 {
            prop_assert!(replica_a.contains(&format!("new-{}", i)));
        }
    }
}
```

### 1.4 RGASequence (Multi-Value / Collaborative)

`RGASequence<T>` implements a Replicated Growable Array for collaborative editing. Each element has a unique `RgaId { timestamp, node_id }`.

#### Properties

| Property | Description |
|---|---|
| **Concurrent Insert Detection** | Two users inserting at the same position produce a deterministic interleaving |
| **Merge Correctness** | All elements from all replicas are present after merge |
| **Causal Ordering** | Elements inserted by the same node appear in insertion order |
| **Convergence** | All merge orderings produce the same final sequence |
| **Delete Tombstones** | Deleted elements never reappear after merge |

#### Pseudocode

```rust
proptest! {
    #[test]
    fn rga_convergence_all_permutations(
        user1_inserts in prop::collection::vec(any::<char>(), 1..5),
        user2_inserts in prop::collection::vec(any::<char>(), 1..5),
    ) {
        let mut doc1: RGASequence<char> = RGASequence::new("user1");
        let mut doc2: RGASequence<char> = RGASequence::new("user2");

        // Concurrent inserts
        for ch in &user1_inserts { doc1.push(*ch); }
        for ch in &user2_inserts { doc2.push(*ch); }

        // Merge in both orders
        let mut merged_12 = doc1.clone(); merged_12.merge(&doc2);
        let mut merged_21 = doc2.clone(); merged_21.merge(&doc1);

        let seq_12: Vec<char> = merged_12.iter().collect();
        let seq_21: Vec<char> = merged_21.iter().collect();

        prop_assert_eq!(seq_12, seq_21, "RGA merge not commutative");

        // All elements from both users must be present
        for ch in &user1_inserts {
            prop_assert!(seq_12.contains(ch));
        }
        for ch in &user2_inserts {
            prop_assert!(seq_12.contains(ch));
        }
    }
}
```

### 1.5 MVRegister (Multi-Value Register)

While Streamline currently uses LWW as the primary register type, the MVRegister (planned in M3 P2 per `src/edge/crdt.rs`) preserves all concurrent writes as siblings until application-level resolution.

#### Properties

| Property | Description |
|---|---|
| **Concurrent Write Detection** | Concurrent writes to the same key produce a multi-value result (siblings) |
| **Merge Correctness** | `merge(a, b)` contains the union of concurrent values |
| **Causal Ordering** | A write that causally follows another replaces (not adds to) it |
| **Sibling Pruning** | When a write dominates another via vector clock, the dominated value is pruned |
| **Convergence** | All replicas converge to the same set of siblings |

#### Pseudocode

```rust
#[test]
fn mvregister_concurrent_writes_produce_siblings() {
    let mut vc_a = VectorClock::new();
    vc_a.increment("node-a");

    let mut vc_b = VectorClock::new();
    vc_b.increment("node-b");

    // vc_a and vc_b are concurrent (neither dominates)
    assert!(vc_a.is_concurrent(&vc_b));

    let write_a = MVWrite { value: "alice", vclock: vc_a };
    let write_b = MVWrite { value: "bob", vclock: vc_b };

    let merged = mvregister_merge(write_a, write_b);
    // Both values preserved as siblings
    assert_eq!(merged.values(), vec!["alice", "bob"]);
}

#[test]
fn mvregister_causal_write_replaces() {
    let mut vc1 = VectorClock::new();
    vc1.increment("node-a");

    // vc2 causally follows vc1
    let mut vc2 = vc1.clone();
    vc2.increment("node-a");

    assert!(vc1.happens_before(&vc2));

    let write1 = MVWrite { value: "old", vclock: vc1 };
    let write2 = MVWrite { value: "new", vclock: vc2 };

    let merged = mvregister_merge(write1, write2);
    // Causal successor replaces predecessor
    assert_eq!(merged.values(), vec!["new"]);
}
```

---

## 2. Partition Simulation Scenarios

All partition tests run against a cluster of 3–5 Streamline nodes communicating via the Kafka wire protocol (port 9092). The test harness uses `iptables`/`tc` (Linux) or the Streamline chaos framework (`tests/chaos/`) to inject faults.

### 2.1 Symmetric Network Partition

**Scenario:** The cluster splits into two halves, each receiving concurrent writes.

```
 ┌───────────────┐          ╳          ┌───────────────┐
 │  Partition A  │      (blocked)      │  Partition B  │
 │  node-1       │                     │  node-3       │
 │  node-2       │                     │  node-4       │
 │               │                     │  node-5       │
 └───────────────┘                     └───────────────┘
```

#### Test Steps

```
1. SETUP:
   - Start 5-node Streamline cluster
   - Create CRDT topic "sym-partition" with all CRDT types
   - Establish baseline: all nodes synced

2. PARTITION:
   - Block ALL traffic between {node-1, node-2} and {node-3, node-4, node-5}
   - Verify: nodes within each partition can communicate
   - Verify: cross-partition traffic is dropped (probe packets)

3. DIVERGE (run for 60 seconds):
   Partition A writes:
     - GCounter("page-views"): increment by 100 on node-1, 200 on node-2
     - LWWRegister("status"): set to "active-A" on node-1
     - ORSet("tags"): add {"alpha", "beta"} on node-2
   Partition B writes:
     - GCounter("page-views"): increment by 300 on node-3
     - LWWRegister("status"): set to "active-B" on node-4 (later timestamp)
     - ORSet("tags"): add {"gamma"}, remove {"alpha"} on node-5

4. HEAL:
   - Remove partition (restore all network links)
   - Wait for convergence (max 30s timeout)

5. VERIFY (on every node):
   - GCounter("page-views").value() == 600  (100 + 200 + 300)
   - LWWRegister("status").value() == "active-B"  (later timestamp wins)
   - ORSet("tags").elements() == {"alpha", "beta", "gamma"}
     (add-wins: node-5's remove only affects tags seen before partition;
      node-2's "alpha" add with a fresh tag survives)
   - All nodes return identical CrdtValue hashes
```

### 2.2 Asymmetric Partition (One-Way Communication)

**Scenario:** node-3 can receive from node-1 and node-2, but cannot send back.

```
 node-1 ──────►  node-3 (receives but cannot send)
 node-2 ──────►  node-3
 node-1 ◄──╳──  node-3 (blocked)
```

#### Test Steps

```
1. Configure one-way iptables: DROP outbound from node-3 to node-1 and node-2
2. node-1 writes GCounter increments; node-3 writes concurrent GCounter increments
3. node-3 receives node-1's state (one-way replication works)
4. node-1 does NOT receive node-3's updates
5. Heal: restore bidirectional communication
6. VERIFY:
   - After heal, all nodes converge within 10s
   - GCounter value reflects sum of all increments from all nodes
   - No data loss from the asymmetric period
```

### 2.3 Rolling Partitions (Sequential Node Isolation)

**Scenario:** Nodes are isolated one at a time in sequence, each accumulating writes while isolated.

#### Test Steps

```
1. 5-node cluster, all synced

2. Round-robin isolation:
   for node_id in [1, 2, 3, 4, 5]:
     a. Isolate node_id from all others
     b. Write 1000 GCounter increments on isolated node
     c. Write 500 GCounter increments on remaining cluster
     d. Hold partition for 30 seconds
     e. Heal partition
     f. Wait for convergence (max 15s)
     g. ASSERT: all nodes agree on GCounter value
     h. Record convergence time

3. FINAL VERIFY:
   - GCounter total == 5 * 1000 + 5 * 500 = 7500
   - All 5 nodes report identical state hash
   - No merge errors in any node's logs
   - Maximum convergence time < 15s per round
```

### 2.4 Split-Brain with Concurrent Conflicting Writes

**Scenario:** Two partitions write conflicting values to the same keys.

#### Test Steps

```
1. 4-node cluster splits into {node-1, node-2} and {node-3, node-4}

2. Both partitions write to the SAME keys:
   Partition A:
     LWWRegister("config"): "version-A-1" → "version-A-2" → "version-A-3"
     ORSet("features"): add "feature-x", remove "feature-y"
     PNCounter("balance"): +100, -30

   Partition B:
     LWWRegister("config"): "version-B-1" → "version-B-2"
     ORSet("features"): add "feature-y", add "feature-z"
     PNCounter("balance"): +50, -10

3. Heal partition

4. VERIFY:
   - LWWRegister("config"): value is whichever write had the latest HLC
     (deterministic; if HLC ties, higher node_id wins)
   - ORSet("features"): {"feature-x", "feature-y", "feature-z"}
     (add-wins semantics: feature-y was concurrently added and removed,
      the add from Partition B has a fresh tag → it survives)
   - PNCounter("balance"): (100 - 30) + (50 - 10) = 110
   - All 4 nodes agree
```

### 2.5 Cascading Partition (Partition Within a Partition)

**Scenario:** A partition forms, then a second partition forms within one half.

```
Phase 1:  {node-1, node-2, node-3} | {node-4, node-5}
Phase 2:  {node-1} | {node-2, node-3} | {node-4, node-5}
Heal:     Phase 2 heals first, then Phase 1
```

#### Test Steps

```
1. Phase 1: Partition {1,2,3} from {4,5}
   - All groups write GCounter and ORSet updates for 30s

2. Phase 2: Further isolate node-1 from {2,3}
   - node-1 writes solo for 30s
   - {2,3} write for 30s
   - {4,5} write for 30s

3. Heal Phase 2: node-1 rejoins {2,3}
   - Wait for convergence within {1,2,3}
   - ASSERT: {1,2,3} all agree

4. Heal Phase 1: {1,2,3} reconnects with {4,5}
   - Wait for full convergence
   - ASSERT: all 5 nodes agree

5. VERIFY:
   - GCounter value == sum of ALL increments across all phases
   - ORSet contains union of all adds minus only removes with matching tags
   - Zero data loss
   - Convergence after each heal < 20s
```

---

## 3. Convergence Verification Approach

### 3.1 Strong Eventual Consistency (SEC) Assertion

**Invariant:** If two replicas have received the same set of updates (in any order), their states are identical.

```rust
/// Assert SEC across all nodes in the cluster.
async fn assert_sec(
    nodes: &[StreamlineNode],
    topic: &str,
    partition: i32,
    key: &[u8],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;

    loop {
        let states: Vec<MergedState> = futures::future::join_all(
            nodes.iter().map(|n| n.get_merged_state(topic, partition, key))
        ).await.into_iter().collect::<Result<Vec<_>>>()?;

        // Compute state hashes
        let hashes: HashSet<u64> = states.iter()
            .map(|s| hash_crdt_value(&s.value))
            .collect();

        if hashes.len() == 1 {
            return Ok(()); // All nodes converged
        }

        if Instant::now() > deadline {
            // Emit detailed divergence report
            for (i, state) in states.iter().enumerate() {
                eprintln!("  Node {}: hash={:#x}, value={:?}, clock={:?}",
                    i, hash_crdt_value(&state.value), state.value, state.merged_clock);
            }
            return Err(anyhow!("SEC violation: {} distinct states after {:?}",
                hashes.len(), timeout));
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
```

### 3.2 Convergence Time Measurement & SLA

| Metric | Target SLA | Measurement Method |
|---|---|---|
| **Local merge latency** | < 1ms per merge op | Instrument `CrdtValue::merge()` with `Instant::now()` |
| **Post-partition convergence** | < 10s for 10K records | Wall clock from partition heal to SEC assertion pass |
| **Post-offline convergence** | < 60s for 7-day backlog | Wall clock from reconnect to SEC pass |
| **Geo-replication convergence** | < 5s + network RTT | Measured via `GeoReplicationManager` lag metrics |

```rust
struct ConvergenceMetrics {
    partition_heal_time: Instant,
    first_merge_time: Option<Instant>,
    full_convergence_time: Option<Instant>,
    records_synced: u64,
    bytes_synced: u64,
}

impl ConvergenceMetrics {
    fn record_convergence(&mut self) {
        self.full_convergence_time = Some(Instant::now());
        let elapsed = self.full_convergence_time.unwrap() - self.partition_heal_time;
        info!(
            convergence_ms = elapsed.as_millis(),
            records = self.records_synced,
            bytes = self.bytes_synced,
            "Convergence achieved"
        );
        assert!(elapsed < Duration::from_secs(10), "Convergence SLA breached");
    }
}
```

### 3.3 Hash-Based State Comparison Across Nodes

Every CRDT value is hashed using a canonical serialization to detect divergence:

```rust
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

fn hash_crdt_value(value: &CrdtValue) -> u64 {
    let mut hasher = DefaultHasher::new();

    match value {
        CrdtValue::GCounter(c) => {
            "gcounter".hash(&mut hasher);
            // Sort node entries for deterministic hashing
            let mut entries: Vec<_> = c.counters().iter().collect();
            entries.sort_by_key(|(k, _)| k.clone());
            for (node, count) in entries {
                node.hash(&mut hasher);
                count.hash(&mut hasher);
            }
        }
        CrdtValue::ORSetString(s) => {
            "orset".hash(&mut hasher);
            let mut elements: Vec<_> = s.elements().into_iter().collect();
            elements.sort();
            elements.hash(&mut hasher);
        }
        // ... other CRDT types
        _ => {
            let json = serde_json::to_string(value).unwrap();
            json.hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Periodic background convergence checker
async fn convergence_watchdog(
    nodes: &[StreamlineNode],
    topic: &str,
    keys: &[Bytes],
    check_interval: Duration,
) {
    loop {
        for key in keys {
            let hashes: Vec<u64> = futures::future::join_all(
                nodes.iter().map(|n| async {
                    let state = n.get_merged_state(topic, 0, key).await.unwrap();
                    hash_crdt_value(&state.value)
                })
            ).await;

            let unique: HashSet<_> = hashes.iter().collect();
            if unique.len() > 1 {
                warn!(key = ?key, hashes = ?hashes, "Convergence divergence detected");
            }
        }
        tokio::time::sleep(check_interval).await;
    }
}
```

### 3.4 Linearizability Checker Integration

For LWW registers and operations that have a total order, integrate a linearizability checker (e.g., Jepsen's `knossos` or `porcupine` in Go):

```
1. Record all operations with wall-clock timestamps:
   { op: "write", key: "config", value: "v1", node: "n1", start_ts, end_ts }
   { op: "read",  key: "config", value: "v2", node: "n3", start_ts, end_ts }

2. Feed operation history to linearizability checker:
   - For LWW registers: verify reads are consistent with a sequential history
     respecting HLC ordering
   - For counters: verify monotonicity (reads never decrease on a single node)

3. Report any violations with the exact operation sequence that broke linearizability
```

```rust
#[derive(Debug, Serialize)]
struct Operation {
    op_type: OpType,
    key: String,
    value: Option<String>,
    node: String,
    start_ns: u64,
    end_ns: u64,
}

#[derive(Debug, Serialize)]
enum OpType { Write, Read }

/// Record operations and dump for external linearizability checker
struct LinearizabilityRecorder {
    ops: Vec<Operation>,
}

impl LinearizabilityRecorder {
    fn record_write(&mut self, key: &str, value: &str, node: &str,
                     start: Instant, end: Instant) {
        self.ops.push(Operation {
            op_type: OpType::Write,
            key: key.to_string(),
            value: Some(value.to_string()),
            node: node.to_string(),
            start_ns: start.elapsed().as_nanos() as u64,
            end_ns: end.elapsed().as_nanos() as u64,
        });
    }

    fn dump_edn(&self, path: &str) -> Result<()> {
        // Export in EDN format for Jepsen/knossos
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, &self.ops)?;
        Ok(())
    }
}
```

### 3.5 Causal Consistency Validation

Leverage Streamline's `VectorClock` and `CausalTimestamp` infrastructure:

```rust
/// Validate causal consistency: if operation A causally precedes B,
/// then any replica that has seen B must also have seen A.
async fn validate_causal_consistency(
    nodes: &[StreamlineNode],
    operations: &[CausalOperation],
) -> Result<()> {
    for op in operations {
        if let Some(dep) = &op.causal_dependency {
            // For each node that has seen `op`, verify it has also seen `dep`
            for node in nodes {
                let node_clock = node.get_vector_clock().await?;
                let op_clock = &op.vector_clock;
                let dep_clock = &dep.vector_clock;

                if node_clock.dominates(op_clock) {
                    // Node has seen `op`, so it must also have seen `dep`
                    assert!(
                        node_clock.dominates(dep_clock),
                        "Causal consistency violation on {}: saw op {:?} but \
                         missing dependency {:?}",
                        node.id(), op_clock, dep_clock
                    );
                }
            }
        }
    }
    Ok(())
}
```

---

## 4. 7-Day Offline Reconnect Test Plan

This test validates Streamline's edge-first architecture (`src/edge/`) where edge nodes with SQLite storage operate offline for extended periods and then reconnect to the cloud.

### 4.1 Test Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Cloud Cluster                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │ cloud-1  │  │ cloud-2  │  │ cloud-3  │                      │
│  │ (active) │  │ (active) │  │ (standby)│                      │
│  └──────────┘  └──────────┘  └──────────┘                      │
└───────────────────────┬─────────────────────────────────────────┘
                        │ (disconnected for 7 days)
                        ╳
                        │
┌───────────────────────┴─────────────────────────────────────────┐
│                      Edge Node (offline)                        │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Streamline Edge Runtime                                   │ │
│  │  ├── Local Kafka protocol listener (:9092)                 │ │
│  │  ├── SQLite WAL storage (bounded ring buffer)              │ │
│  │  ├── CRDT merge engine (LWW, GCounter, ORSet, RGA)        │ │
│  │  ├── EdgeSyncCheckpoint (resumable sync state)             │ │
│  │  └── HLC clock (continues ticking offline)                 │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Phase 1: Baseline Establishment (Day 0)

```
1. Start 3-node cloud cluster + 1 edge node
2. Create CRDT topics:
   - "sensor-data"   (GCounter for accumulation)
   - "device-config" (LWWRegister for settings)
   - "alerts"        (ORSet for active alerts)
   - "audit-log"     (RGASequence for ordered events)
3. Seed initial data:
   - GCounter("total-readings"): 10,000
   - LWWRegister("firmware-version"): "v1.0.0"
   - ORSet("active-alerts"): {"overheat-001", "low-battery-002"}
4. Verify all nodes (cloud + edge) are synced
5. Record baseline state hashes for all keys
```

### 4.3 Phase 2: Offline Accumulation (Days 1–7)

```
EDGE NODE (offline, writing to local SQLite):
  Day 1: GCounter("total-readings") += 15,000 (sensor ticks)
  Day 2: GCounter("total-readings") += 14,500
         LWWRegister("firmware-version") → "v1.1.0" (local OTA)
  Day 3: GCounter("total-readings") += 16,200
         ORSet("active-alerts").add("pressure-003")
  Day 4: GCounter("total-readings") += 13,800
         ORSet("active-alerts").remove("overheat-001")
  Day 5: GCounter("total-readings") += 15,500
         RGASequence("audit-log").push("maintenance-check")
  Day 6: GCounter("total-readings") += 14,900
         LWWRegister("firmware-version") → "v1.2.0" (second OTA)
  Day 7: GCounter("total-readings") += 15,100
         ORSet("active-alerts").add("sensor-drift-004")

CLOUD CLUSTER (online, receiving writes from other sources):
  Day 1-7: GCounter("total-readings") += 50,000/day from other edges
           LWWRegister("firmware-version") → "v2.0.0" (Day 5, central push)
           ORSet("active-alerts").add("network-005")
           ORSet("active-alerts").remove("low-battery-002")
           RGASequence("audit-log").push("central-maintenance-day-3")

CLOCK DRIFT SIMULATION:
  - Edge node's system clock drifts +3 seconds over 7 days
  - HLC compensates via logical counter advancement
  - Inject NTP-style correction on Day 4 (-1.5s jump)
```

### 4.4 Phase 3: Reconnection and Sync (Day 8)

```
1. RECONNECT:
   - Restore network link between edge and cloud
   - Edge's EdgeSyncEngine detects connectivity via health probe to cloud:9094
   - Sync begins from EdgeSyncCheckpoint (last known offset per topic-partition)

2. SYNC PROTOCOL:
   a. Edge sends delta of all locally accumulated CRDT records since checkpoint
   b. Cloud sends delta of all records edge has missed
   c. Both sides apply CrdtStateManager.process_record() for each received record
   d. Merge happens automatically via CRDT merge semantics

3. EXPECTED SYNC VOLUME:
   - Edge → Cloud: ~7 days × ~15,000 records/day ≈ 105,000 records
   - Cloud → Edge: ~7 days × 50,000 records/day ≈ 350,000 records
   - Wire format: CRDT headers + JSON values ≈ 200 bytes/record avg
   - Total transfer: ~91 MB (pre-compression), ~15 MB (with compression)

4. SYNC STAGES:
   Stage 1: Checkpoint exchange (edge sends last known offsets)
   Stage 2: Cloud sends missed records in batches of 10,000
   Stage 3: Edge sends locally accumulated records
   Stage 4: Bidirectional catchup until both sides are current
   Stage 5: Real-time replication resumes
```

### 4.5 Phase 4: Data Integrity Verification

```rust
async fn verify_7day_reconnect(
    cloud_nodes: &[StreamlineNode],
    edge_node: &StreamlineNode,
) -> Result<()> {
    let all_nodes = [cloud_nodes, &[edge_node.clone()]].concat();

    // 1. GCounter must reflect sum of ALL increments
    let expected_total = 10_000            // baseline
        + (15_000 + 14_500 + 16_200 + 13_800 + 15_500 + 14_900 + 15_100)  // edge
        + (50_000 * 7);                    // cloud (other edges)
    assert_sec_value(&all_nodes, "sensor-data", "total-readings",
        |v| matches!(v, CrdtValue::GCounter(c) if c.value() == expected_total)
    ).await?;

    // 2. LWWRegister: cloud's "v2.0.0" (Day 5) vs edge's "v1.2.0" (Day 6)
    //    Edge's Day 6 write has a later HLC timestamp → "v1.2.0" wins
    //    UNLESS cloud's write on Day 5 was received by edge before disconnect
    //    (depends on exact timing — the test must record which write has the latest HLC)
    assert_sec_value(&all_nodes, "device-config", "firmware-version",
        |v| matches!(v, CrdtValue::LWWRegisterString(r) if
            r.value() == "v2.0.0" || r.value() == "v1.2.0"
            // Whichever has the latest HLC timestamp
        )
    ).await?;

    // 3. ORSet: verify add-wins semantics across 7-day gap
    assert_sec_value(&all_nodes, "alerts", "active-alerts",
        |v| {
            if let CrdtValue::ORSetString(s) = v {
                // "pressure-003" added by edge Day 3 — must be present
                assert!(s.contains(&"pressure-003".to_string()));
                // "sensor-drift-004" added by edge Day 7 — must be present
                assert!(s.contains(&"sensor-drift-004".to_string()));
                // "network-005" added by cloud — must be present
                assert!(s.contains(&"network-005".to_string()));
                // "overheat-001": removed by edge Day 4, but only removes
                //   tags known at time of removal. If cloud re-added it
                //   with a fresh tag, it would survive. Otherwise, removed.
                // "low-battery-002": removed by cloud — should be absent
                //   (unless edge re-added it with fresh tag)
                true
            } else { false }
        }
    ).await?;

    // 4. RGA: all audit entries present in causal order
    assert_sec_value(&all_nodes, "audit-log", "audit-log",
        |v| {
            if let CrdtValue::RGASequenceString(seq) = v {
                let entries: Vec<String> = seq.iter().cloned().collect();
                assert!(entries.contains(&"maintenance-check".to_string()));
                assert!(entries.contains(&"central-maintenance-day-3".to_string()));
                true
            } else { false }
        }
    ).await?;

    Ok(())
}
```

### 4.6 Performance Benchmarks for Large Delta Sync

| Metric | Target | Measurement |
|---|---|---|
| **Sync initiation latency** | < 2s from connectivity detection | Time from first successful health probe to first record transfer |
| **Throughput (edge → cloud)** | ≥ 50,000 records/s | Records transferred per second during bulk sync |
| **Throughput (cloud → edge)** | ≥ 20,000 records/s | Limited by edge hardware (SQLite write speed) |
| **Total sync time (105K records)** | < 30s | Wall clock for complete bidirectional sync |
| **Memory usage during sync** | < 256 MB on edge | RSS measured during peak sync activity |
| **SQLite WAL checkpoint** | < 5s after sync | Time to checkpoint WAL after bulk writes |
| **Compression ratio** | ≥ 5:1 for CRDT records | Wire bytes / uncompressed bytes |

```rust
#[tokio::test]
async fn bench_7day_delta_sync() {
    let (cloud, edge) = setup_offline_scenario().await;

    // Simulate 7 days of accumulated writes
    let records = generate_7day_workload(); // ~105K records
    edge.ingest_locally(records).await;

    // Reconnect and measure
    let start = Instant::now();
    edge.reconnect(&cloud).await;

    let sync_complete = edge.wait_for_sync_complete(Duration::from_secs(60)).await;
    let elapsed = start.elapsed();

    assert!(sync_complete, "Sync did not complete within 60s");
    info!(
        sync_duration_ms = elapsed.as_millis(),
        records_synced = 105_000,
        throughput_rps = 105_000_000 / elapsed.as_millis(),
        "7-day delta sync completed"
    );

    // Verify no data loss
    assert_sec(&[cloud, edge], "sensor-data", 0, b"total-readings",
        Duration::from_secs(10)).await.unwrap();
}
```

### 4.7 Clock Drift Handling During Offline Period

```
Test Matrix:
┌─────────────────────┬──────────────────┬──────────────────────────────┐
│ Drift Scenario      │ Magnitude        │ Expected Behavior            │
├─────────────────────┼──────────────────┼──────────────────────────────┤
│ Forward drift       │ +5s over 7 days  │ HLC physical time advances;  │
│                     │                  │ logical counter stays low    │
├─────────────────────┼──────────────────┼──────────────────────────────┤
│ Backward drift      │ -3s over 7 days  │ HLC logical counter          │
│                     │                  │ compensates; no timestamp    │
│                     │                  │ regression                   │
├─────────────────────┼──────────────────┼──────────────────────────────┤
│ NTP correction jump │ -2s instant      │ HLC detects physical time    │
│                     │                  │ went backward; advances      │
│                     │                  │ logical counter only         │
├─────────────────────┼──────────────────┼──────────────────────────────┤
│ Large forward jump  │ +1 hour          │ HLC accepts new physical     │
│                     │                  │ time; resets logical to 0.   │
│                     │                  │ LWW decisions may be         │
│                     │                  │ affected — test explicitly   │
├─────────────────────┼──────────────────┼──────────────────────────────┤
│ Extreme drift       │ +24 hours        │ HLC bound check should       │
│                     │                  │ reject (configurable max     │
│                     │                  │ drift threshold)             │
└─────────────────────┴──────────────────┴──────────────────────────────┘
```

```rust
#[test]
fn hlc_handles_backward_clock_jump() {
    let mut hlc = HybridLogicalClock {
        physical: 1_000_000,
        logical: 0,
        node_id: "edge-1".to_string(),
    };

    // Simulate clock jumping backward (NTP correction)
    // The tick() method in clock.rs handles this:
    // if now > self.physical → advance physical, reset logical
    // else → increment logical only
    let t1 = hlc.tick();

    // Simulate system clock going backward by mocking current_time_ms
    // HLC should NOT regress — it increments logical counter instead
    let t2 = hlc.tick();

    assert!(t1.happens_before(&t2), "HLC must never go backward");
}

#[test]
fn lww_correctness_under_clock_drift() {
    // Edge node has clock drifted +5 seconds ahead
    let edge_ts = 1_000_005_000; // 5s ahead
    let cloud_ts = 1_000_000_000; // correct time

    let edge_write = LWWRegister::with_timestamp("edge-1", "edge-value", edge_ts);
    let cloud_write = LWWRegister::with_timestamp("cloud-1", "cloud-value", cloud_ts);

    let mut merged = edge_write.clone();
    merged.merge(&cloud_write);

    // Edge's drifted clock makes it "win" — this is a known trade-off of LWW
    // The test documents this behavior explicitly
    assert_eq!(merged.value(), "edge-value");

    // Mitigation: Streamline should log a warning when HLC physical time
    // difference between merging parties exceeds a configurable threshold
}
```

### 4.8 Conflict Resolution for Overlapping Edits

```rust
/// Test: same key edited on both edge and cloud during offline period
#[tokio::test]
async fn overlapping_edits_during_offline() {
    let (cloud, edge) = setup_cluster().await;

    // Both start with same baseline
    let baseline_set: ORSet<String> = ORSet::new("shared");
    sync_initial_state(&cloud, &edge, &baseline_set).await;

    // Disconnect
    partition(&edge, &cloud).await;

    // Edge: add "edge-item-1", "edge-item-2"; remove "shared-item"
    // Cloud: add "cloud-item-1"; remove "shared-item" with different observation
    //
    // Key conflict: both sides remove "shared-item" but at different vector clock
    // states. If either side also re-added it, add-wins applies.

    edge.apply_ops(vec![
        ORSetOp::Add("edge-item-1"),
        ORSetOp::Add("edge-item-2"),
        ORSetOp::Remove("shared-item"),
    ]).await;

    cloud.apply_ops(vec![
        ORSetOp::Add("cloud-item-1"),
        ORSetOp::Remove("shared-item"),
    ]).await;

    // Reconnect
    heal_partition(&edge, &cloud).await;
    wait_for_convergence(&[&cloud, &edge], Duration::from_secs(10)).await;

    // Verify
    let final_state = edge.get_orset("shared-key").await;
    assert!(final_state.contains("edge-item-1"));
    assert!(final_state.contains("edge-item-2"));
    assert!(final_state.contains("cloud-item-1"));
    // "shared-item" removed by both — should be absent
    assert!(!final_state.contains("shared-item"));
}
```

---

## 5. Test Infrastructure & Tooling

### 5.1 Test Harness Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Test Orchestrator                          │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ Cluster     │  │ Fault        │  │ Verification           │ │
│  │ Manager     │  │ Injector     │  │ Engine                 │ │
│  │             │  │              │  │                        │ │
│  │ • Start/    │  │ • iptables   │  │ • SEC checker          │ │
│  │   stop      │  │ • tc netem   │  │ • Hash comparator      │ │
│  │   nodes     │  │ • Clock      │  │ • Linearizability      │ │
│  │ • Config    │  │   skew       │  │   checker              │ │
│  │ • Health    │  │ • Kill -9    │  │ • Causal consistency   │ │
│  │   checks    │  │ • Disk       │  │ • Convergence timer    │ │
│  │             │  │   faults     │  │ • Operation recorder   │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Workload Generators                      ││
│  │  • Random CRDT ops (proptest)                               ││
│  │  • Realistic sensor data (7-day simulation)                 ││
│  │  • Adversarial concurrent writes                            ││
│  │  • High-throughput stress (100K ops/s)                      ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Running Tests

```bash
# Unit property-based tests (fast, no cluster needed)
cargo test --features crdt -- crdt_property_tests --nocapture

# Integration tests (requires Docker for multi-node cluster)
cargo test --features "crdt,edge,clustering" --test crdt_jepsen -- --nocapture

# Partition simulation (requires Linux for iptables, or Docker network manipulation)
STREAMLINE_CHAOS=1 cargo test --test chaos_crdt -- --nocapture

# 7-day offline simulation (accelerated: 7 simulated days in ~10 minutes)
STREAMLINE_OFFLINE_SIM=1 cargo test --test offline_reconnect -- --nocapture

# Full Jepsen-class suite (long-running: ~2 hours)
make test-jepsen-crdt
```

### 5.3 Docker Compose for Test Clusters

```yaml
# tests/docker-compose.crdt-test.yml
services:
  streamline-1:
    image: streamline:latest
    environment:
      STREAMLINE_NODE_ID: node-1
      STREAMLINE_CLUSTER_PEERS: node-2:9093,node-3:9093
      STREAMLINE_CRDT_ENABLED: "true"
    ports: ["19092:9092", "19094:9094"]
    networks: [crdt-test]

  streamline-2:
    image: streamline:latest
    environment:
      STREAMLINE_NODE_ID: node-2
      STREAMLINE_CLUSTER_PEERS: node-1:9093,node-3:9093
      STREAMLINE_CRDT_ENABLED: "true"
    ports: ["29092:9092"]
    networks: [crdt-test]

  streamline-3:
    image: streamline:latest
    environment:
      STREAMLINE_NODE_ID: node-3
      STREAMLINE_CLUSTER_PEERS: node-1:9093,node-2:9093
      STREAMLINE_CRDT_ENABLED: "true"
    ports: ["39092:9092"]
    networks: [crdt-test]

  edge-node:
    image: streamline:latest
    environment:
      STREAMLINE_NODE_ID: edge-1
      STREAMLINE_EDGE_MODE: "true"
      STREAMLINE_CLOUD_ENDPOINT: "streamline-1:9092"
      STREAMLINE_STORAGE_ENGINE: sqlite
    ports: ["49092:9092"]
    networks: [crdt-test]

networks:
  crdt-test:
    driver: bridge
```

### 5.4 CI Integration

```yaml
# .github/workflows/crdt-jepsen.yml
name: CRDT Jepsen Tests
on:
  push:
    paths: ['src/crdt/**', 'src/edge/**', 'src/replication/**']
  schedule:
    - cron: '0 2 * * 1'  # Weekly full suite on Monday 2 AM

jobs:
  property-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo test --features crdt -- crdt_property_tests
        env:
          PROPTEST_CASES: 50000

  partition-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - run: docker compose -f tests/docker-compose.crdt-test.yml up -d
      - run: cargo test --features "crdt,clustering" --test crdt_jepsen
      - run: docker compose -f tests/docker-compose.crdt-test.yml down -v

  offline-reconnect:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4
      - run: docker compose -f tests/docker-compose.crdt-test.yml up -d
      - run: STREAMLINE_OFFLINE_SIM=1 cargo test --test offline_reconnect
      - run: docker compose -f tests/docker-compose.crdt-test.yml down -v
```

---

## 6. Acceptance Criteria

### 6.1 Must-Pass Gates

| # | Criterion | Threshold |
|---|---|---|
| 1 | All property-based tests pass at 50K cases | 0 failures |
| 2 | Symmetric partition: convergence after heal | < 10s |
| 3 | Asymmetric partition: no data loss | 0 lost records |
| 4 | Rolling partition: all rounds converge | 100% rounds |
| 5 | Split-brain: deterministic conflict resolution | Matches CRDT spec |
| 6 | Cascading partition: full convergence | < 30s total |
| 7 | 7-day offline: complete data integrity | 0 data loss |
| 8 | 7-day offline: sync completes | < 60s |
| 9 | Clock drift: HLC never regresses | Monotonic guarantee |
| 10 | Linearizability: LWW reads consistent | 0 violations |
| 11 | Causal consistency: no out-of-order delivery | 0 violations |
| 12 | Hash comparison: all nodes identical post-convergence | 1 unique hash |

### 6.2 Performance Baselines

| Metric | Baseline | Regression Threshold |
|---|---|---|
| Single merge latency (GCounter) | < 100μs | > 500μs |
| Single merge latency (ORSet, 1K elements) | < 1ms | > 5ms |
| Bulk sync throughput | > 50K records/s | < 25K records/s |
| Memory per cached CRDT state | < 1KB (GCounter) | > 5KB |
| SQLite write throughput (edge) | > 20K records/s | < 10K records/s |

### 6.3 Failure Reporting

Every test failure must produce:

1. **Operation history**: full sequence of reads/writes with timestamps
2. **Cluster state dump**: CRDT values on every node at failure time
3. **Vector clock state**: per-node vector clocks for causality debugging
4. **Network event log**: partition/heal events with timestamps
5. **HLC timeline**: physical + logical clock values across all nodes
6. **Diff report**: which nodes diverged and by how much

```rust
struct FailureReport {
    test_name: String,
    failure_type: FailureType, // SEC_Violation, DataLoss, ConvergenceTimeout, etc.
    operation_history: Vec<Operation>,
    node_states: HashMap<String, CrdtValue>,
    vector_clocks: HashMap<String, VectorClock>,
    network_events: Vec<NetworkEvent>,
    hlc_timeline: Vec<(String, HybridLogicalClock)>,
    divergence_details: Option<DivergenceReport>,
}

impl FailureReport {
    fn write_to_file(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}
```
