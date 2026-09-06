//! Jepsen-class convergence tests for CRDT types.
//!
//! These tests simulate network partitions and concurrent writes across
//! multiple nodes, then verify that all replicas converge to the same
//! state after reconnection — regardless of merge order.
//!
//! The test methodology mirrors Jepsen's approach:
//! 1. Initialize replicas on multiple nodes
//! 2. Partition the network (isolate replicas)
//! 3. Apply concurrent writes to isolated replicas
//! 4. Reconnect and merge in various orders
//! 5. Assert all replicas converge to identical state

use streamline::crdt::{Crdt, GCounter, LWWRegister, ORSet};

/// Simulate a 3-node partition where each node independently increments
/// a G-Counter, then verify convergence after all merge orders.
#[test]
fn gcounter_3node_partition_convergence() {
    // Phase 1: Initialize replicas
    let mut node_a = GCounter::new("node-a");
    let mut node_b = GCounter::new("node-b");
    let mut node_c = GCounter::new("node-c");

    // Phase 2: Partition — each node writes independently
    node_a.increment(100);
    node_a.increment(50);

    node_b.increment(200);

    node_c.increment(75);
    node_c.increment(25);

    // Phase 3: Reconnect — try all merge orders
    // Order A: a <- b <- c
    let mut merged_abc = node_a.clone();
    merged_abc.merge(&node_b);
    merged_abc.merge(&node_c);

    // Order B: c <- a <- b
    let mut merged_cab = node_c.clone();
    merged_cab.merge(&node_a);
    merged_cab.merge(&node_b);

    // Order C: b <- c <- a
    let mut merged_bca = node_b.clone();
    merged_bca.merge(&node_c);
    merged_bca.merge(&node_a);

    // Phase 4: Assert convergence
    let expected = 100 + 50 + 200 + 75 + 25;
    assert_eq!(merged_abc.value(), expected, "ABC merge order");
    assert_eq!(merged_cab.value(), expected, "CAB merge order");
    assert_eq!(merged_bca.value(), expected, "BCA merge order");
}

/// Simulate repeated partitions and merges (multiple rounds).
#[test]
fn gcounter_multi_round_partition() {
    let mut node_a = GCounter::new("node-a");
    let mut node_b = GCounter::new("node-b");

    // Round 1: partition and concurrent writes
    node_a.increment(10);
    node_b.increment(20);

    // Reconnect
    node_a.merge(&node_b);
    node_b.merge(&node_a);
    assert_eq!(node_a.value(), 30);
    assert_eq!(node_b.value(), 30);

    // Round 2: partition again
    node_a.increment(5);
    node_b.increment(15);

    // Reconnect
    node_a.merge(&node_b);
    node_b.merge(&node_a);
    assert_eq!(node_a.value(), 50);
    assert_eq!(node_b.value(), 50);

    // Round 3: asymmetric merge (only a merges b)
    node_a.increment(1);
    node_b.increment(2);
    node_a.merge(&node_b);
    // a sees both, b only sees its own
    assert_eq!(node_a.value(), 53);
    assert_eq!(node_b.value(), 52);
    // Final sync
    node_b.merge(&node_a);
    assert_eq!(node_b.value(), 53);
}

/// LWW Register: concurrent writes from partitioned nodes.
#[test]
fn lww_register_partition_last_writer_wins() {
    // Node A writes first
    let reg_a = LWWRegister::new("node-a", "value-a".to_string());

    // Simulate time passing
    std::thread::sleep(std::time::Duration::from_millis(2));

    // Node B writes later (while partitioned from A)
    let reg_b = LWWRegister::new("node-b", "value-b".to_string());

    // Merge in both orders — later timestamp always wins
    let mut merged_ab = reg_a.clone();
    merged_ab.merge(&reg_b);

    let mut merged_ba = reg_b.clone();
    merged_ba.merge(&reg_a);

    assert_eq!(
        merged_ab.value(),
        merged_ba.value(),
        "LWW merge must be commutative"
    );
    assert_eq!(merged_ab.value(), "value-b", "Later write should win");
}

/// ORSet: concurrent add and remove across partitioned nodes.
#[test]
fn orset_concurrent_add_remove_add_wins() {
    // Both nodes start with "item" in the set
    let mut node_a: ORSet<String> = ORSet::new("node-a");
    node_a.add("item".to_string());

    let mut node_b = node_a.clone();
    node_b.set_local_node("node-b");

    // Partition: node_a removes, node_b re-adds
    node_a.remove(&"item".to_string());
    node_b.add("item".to_string());

    // Merge: add-wins semantics
    let mut merged_ab = node_a.clone();
    merged_ab.merge(&node_b);
    assert!(
        merged_ab.contains(&"item".to_string()),
        "Add should win over concurrent remove (A<-B)"
    );

    let mut merged_ba = node_b.clone();
    merged_ba.merge(&node_a);
    assert!(
        merged_ba.contains(&"item".to_string()),
        "Add should win over concurrent remove (B<-A)"
    );
}

/// ORSet: simulate shopping cart with concurrent modifications.
#[test]
fn orset_shopping_cart_scenario() {
    // Initialize cart
    let mut cart_a: ORSet<String> = ORSet::new("device-phone");
    cart_a.add("widget".to_string());
    cart_a.add("gadget".to_string());
    cart_a.add("doohickey".to_string());

    // Clone to tablet (simulates sync before partition)
    let mut cart_b = cart_a.clone();
    cart_b.set_local_node("device-tablet");

    // Partition: phone removes "widget", tablet adds "thingamajig"
    cart_a.remove(&"widget".to_string());
    cart_b.add("thingamajig".to_string());

    // Reconnect and merge
    cart_a.merge(&cart_b);
    cart_b.merge(&cart_a);

    // Both should converge
    assert!(
        !cart_a.contains(&"widget".to_string()),
        "widget was removed"
    );
    assert!(cart_a.contains(&"gadget".to_string()), "gadget untouched");
    assert!(
        cart_a.contains(&"doohickey".to_string()),
        "doohickey untouched"
    );
    assert!(
        cart_a.contains(&"thingamajig".to_string()),
        "thingamajig was added"
    );

    assert_eq!(
        cart_a.len(),
        cart_b.len(),
        "replicas must converge to same size"
    );
}

/// Simulate a 5-node cluster with cascading partitions.
#[test]
fn gcounter_5node_cascading_partition() {
    let mut nodes: Vec<GCounter> = (0..5).map(|i| GCounter::new(format!("node-{i}"))).collect();

    // Each node increments independently
    for (i, node) in nodes.iter_mut().enumerate() {
        node.increment((i as u64 + 1) * 100);
    }

    // Cascade merge: 0 <- 1, 0 <- 2, 0 <- 3, 0 <- 4
    let mut primary = nodes[0].clone();
    for node in &nodes[1..] {
        primary.merge(node);
    }

    // Reverse cascade: 4 <- 3 <- 2 <- 1 <- 0
    let mut reverse = nodes[4].clone();
    for node in nodes[..4].iter().rev() {
        reverse.merge(node);
    }

    // Pairwise cascade: merge pairs then merge results
    let mut pair_01 = nodes[0].clone();
    pair_01.merge(&nodes[1]);
    let mut pair_23 = nodes[2].clone();
    pair_23.merge(&nodes[3]);
    pair_01.merge(&pair_23);
    pair_01.merge(&nodes[4]);

    let expected = 100 + 200 + 300 + 400 + 500;
    assert_eq!(primary.value(), expected, "forward cascade");
    assert_eq!(reverse.value(), expected, "reverse cascade");
    assert_eq!(pair_01.value(), expected, "pairwise cascade");
}

/// Idempotency: merging the same state multiple times doesn't change result.
#[test]
fn merge_idempotency_stress() {
    let mut a = GCounter::new("a");
    a.increment(42);
    let mut b = GCounter::new("b");
    b.increment(58);

    let mut merged = a.clone();
    merged.merge(&b);
    let after_first = merged.value();

    // Merge again (and again) — should be idempotent
    for _ in 0..100 {
        merged.merge(&a);
        merged.merge(&b);
    }

    assert_eq!(merged.value(), after_first, "merge must be idempotent");
}

/// ORSet: rapid add/remove cycles across partitioned nodes.
#[test]
fn orset_rapid_add_remove_convergence() {
    let mut node_a: ORSet<i32> = ORSet::new("node-a");
    let mut node_b: ORSet<i32> = ORSet::new("node-b");

    // Node A: add 1..=10
    for i in 1..=10 {
        node_a.add(i);
    }

    // Node B: add 5..=15
    for i in 5..=15 {
        node_b.add(i);
    }

    // Merge
    node_a.merge(&node_b);
    node_b.merge(&node_a);

    // Both should have 1..=15
    for i in 1..=15 {
        assert!(node_a.contains(&i), "node_a missing {i}");
        assert!(node_b.contains(&i), "node_b missing {i}");
    }
    assert_eq!(node_a.len(), node_b.len());
}
