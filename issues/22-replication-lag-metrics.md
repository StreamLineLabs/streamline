# Add Replication Lag Metrics

## Priority: Medium

## Summary

Add metrics to track replication lag between leader and followers. Essential for monitoring cluster health and data durability in production.

## Current State

- `src/metrics/mod.rs` has basic metrics
- `src/replication/` tracks ISR membership
- `src/replication/watermarks.rs` tracks high watermark
- No metrics exposed for replication lag
- No visibility into follower catch-up progress

## Requirements

### Metrics to Add

```
# Replication lag in messages
streamline_partition_replication_lag_messages{topic="x", partition="0", follower="2"}

# Replication lag in bytes
streamline_partition_replication_lag_bytes{topic="x", partition="0", follower="2"}

# Time since last replication (seconds)
streamline_partition_last_replication_seconds{topic="x", partition="0", follower="2"}

# ISR size per partition
streamline_partition_isr_size{topic="x", partition="0"}

# Under-replicated partitions count
streamline_under_replicated_partitions{}

# Offline partitions count (no leader)
streamline_offline_partitions{}
```

### Calculation Logic

```rust
// Replication lag = Leader LEO - Follower LEO
let lag_messages = leader_log_end_offset - follower_log_end_offset;

// Under-replicated = ISR size < replication factor
let under_replicated = isr_size < replication_factor;
```

### Implementation Tasks

1. Add new metrics to `src/metrics/mod.rs`
2. Track follower offsets in replication sender
3. Calculate and record lag on each replication response
4. Add ISR size gauge per partition
5. Add under-replicated partition counter
6. Update metrics on ISR changes
7. Add tests for metric accuracy

### Metric Update Points

- **On replication fetch response**: Update follower offset, calculate lag
- **On ISR change**: Update ISR size gauge, under-replicated count
- **On leader change**: Reset/update partition metrics
- **Periodic (every 10s)**: Refresh all lag metrics

### Alert Recommendations

```yaml
# High replication lag
- alert: HighReplicationLag
  expr: streamline_partition_replication_lag_messages > 10000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High replication lag on {{ $labels.topic }}-{{ $labels.partition }}"

# Under-replicated partitions
- alert: UnderReplicatedPartitions
  expr: streamline_under_replicated_partitions > 0
  for: 5m
  labels:
    severity: warning

# Stale replication
- alert: StaleReplication
  expr: streamline_partition_last_replication_seconds > 60
  for: 1m
  labels:
    severity: critical
```

### Dashboard Panel Suggestions

1. **Replication Lag Heatmap**: Topic/partition grid colored by lag
2. **Under-replicated Partitions**: Single stat showing count
3. **ISR Size Distribution**: Histogram of ISR sizes
4. **Lag Over Time**: Time series of max lag per topic

### Acceptance Criteria

- [ ] Lag metrics update in real-time
- [ ] ISR size metric accurate
- [ ] Under-replicated count correct
- [ ] Metrics survive leader changes
- [ ] Minimal performance impact
- [ ] Documentation updated with new metrics
- [ ] Example alert rules provided

## Related Files

- `src/metrics/mod.rs` - Metrics definitions
- `src/replication/sender.rs` - Leader replication
- `src/replication/fetcher.rs` - Follower replication
- `src/replication/isr.rs` - ISR management
- `src/replication/watermarks.rs` - Offset tracking

## Labels

`medium-priority`, `observability`, `cluster`, `production-readiness`
