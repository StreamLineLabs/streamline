# Implement Chaos Testing for Cluster Resilience

## Priority: High

## Summary

Add chaos engineering tests to validate cluster behavior under failure conditions. The current test suite covers happy paths but lacks validation of failure scenarios like network partitions, node crashes, and split-brain conditions.

## Current State

- `tests/cluster_test.rs` - Tests basic cluster bootstrapping and leadership
- No tests for network partition scenarios
- No tests for node failure during replication
- No tests for split-brain prevention
- No tests for recovery after failures

## Requirements

### Test Scenarios

#### Network Partitions
1. **Leader isolation** - Leader loses connectivity to followers
   - Expected: New leader elected, old leader steps down on reconnect
2. **Follower isolation** - Single follower loses connectivity
   - Expected: ISR shrinks, replication continues with remaining nodes
3. **Minority partition** - Cluster splits into minority/majority
   - Expected: Only majority partition accepts writes

#### Node Failures
1. **Leader crash** - Leader process terminates unexpectedly
   - Expected: Failover completes within timeout, no data loss for acked writes
2. **Follower crash** - Follower terminates during replication
   - Expected: ISR updated, replication continues
3. **Cascading failures** - Multiple nodes fail in sequence
   - Expected: Cluster remains available if quorum maintained

#### Recovery Scenarios
1. **Node rejoin** - Failed node comes back online
   - Expected: Catches up via replication, rejoins ISR
2. **Data reconciliation** - Node with stale data rejoins
   - Expected: Truncates divergent data, syncs from leader
3. **Full cluster restart** - All nodes restart simultaneously
   - Expected: Cluster recovers, elects leader, resumes operation

### Implementation Approach

#### Test Framework
```rust
// tests/chaos/mod.rs
pub struct ChaosCluster {
    nodes: Vec<TestNode>,
    network: NetworkSimulator,
}

impl ChaosCluster {
    pub async fn partition(&mut self, groups: Vec<Vec<NodeId>>);
    pub async fn heal_partition(&mut self);
    pub async fn kill_node(&mut self, node_id: NodeId);
    pub async fn restart_node(&mut self, node_id: NodeId);
    pub async fn slow_network(&mut self, latency_ms: u64);
}
```

#### Network Simulation
- Use `tokio::sync::mpsc` channels between nodes
- Inject failures by dropping/delaying messages
- Simulate partial connectivity (node A can reach B but not C)

### Implementation Tasks

1. Create `tests/chaos/` test module
2. Implement `NetworkSimulator` for controlled failures
3. Implement `TestNode` wrapper for lifecycle control
4. Write network partition tests
5. Write node failure tests
6. Write recovery tests
7. Add CI job for chaos tests (longer timeout)
8. Document failure modes and expected behavior

### Test Configuration

```rust
#[tokio::test]
#[ignore] // Run with: cargo test --ignored
async fn test_leader_isolation() {
    let cluster = ChaosCluster::new(3).await;

    // Produce some data
    cluster.produce("test-topic", b"message").await;

    // Isolate leader
    let leader = cluster.current_leader();
    cluster.partition(vec![vec![leader], vec![/* others */]]).await;

    // Verify new leader elected
    cluster.wait_for_leader_change(Duration::from_secs(10)).await;

    // Verify writes succeed to new leader
    cluster.produce("test-topic", b"message2").await;

    // Heal partition
    cluster.heal_partition().await;

    // Verify old leader steps down and syncs
    cluster.wait_for_isr_size("test-topic", 0, 3).await;
}
```

### Acceptance Criteria

- [ ] Network partition tests pass reliably
- [ ] Node failure tests pass reliably
- [ ] Recovery tests verify data integrity
- [ ] Tests complete within reasonable time (< 5 min each)
- [ ] Failure modes documented
- [ ] CI runs chaos tests on merge to main

## Related Files

- `tests/cluster_test.rs` - Existing cluster tests
- `src/cluster/mod.rs` - Cluster membership
- `src/cluster/raft.rs` - Consensus protocol
- `src/replication/` - Data replication

## Labels

`high-priority`, `testing`, `cluster`, `production-readiness`
