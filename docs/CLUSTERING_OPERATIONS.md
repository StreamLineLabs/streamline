# Clustering Operations Guide

## Overview

Streamline uses Raft consensus (via OpenRaft) for multi-node clustering. This guide covers operational procedures for production clusters.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Node 1     │    │   Node 2     │    │   Node 3     │
│  (Leader)    │◄──►│  (Follower)  │◄──►│  (Follower)  │
│              │    │              │    │              │
│ Raft Log     │    │ Raft Log     │    │ Raft Log     │
│ Partitions   │    │ Partitions   │    │ Partitions   │
│ [1,3,5]      │    │ [2,4,6]      │    │ [1,2,3]      │
└──────────────┘    └──────────────┘    └──────────────┘
```

## Cluster Formation

### Initial Setup (3-node)
```bash
# Node 1 (seed node)
streamline --node-id 1 \
  --listen-addr 10.0.0.1:9092 \
  --inter-broker-addr 10.0.0.1:9093 \
  --seed-nodes "10.0.0.1:9093"

# Node 2
streamline --node-id 2 \
  --listen-addr 10.0.0.2:9092 \
  --inter-broker-addr 10.0.0.2:9093 \
  --seed-nodes "10.0.0.1:9093"

# Node 3
streamline --node-id 3 \
  --listen-addr 10.0.0.3:9092 \
  --inter-broker-addr 10.0.0.3:9093 \
  --seed-nodes "10.0.0.1:9093"
```

### Verify Cluster Health
```bash
streamline-cli cluster status
# Expected: 3 nodes, 1 leader, 2 followers

streamline-cli cluster describe
# Shows: node IDs, roles, partition assignments, replication status
```

## Failure Modes and Recovery

### Leader Failure
**Behavior:** Raft elects a new leader within the election timeout.
**Expected recovery time:** < 30 seconds
**Data impact:** Zero data loss (committed writes are replicated)

**Runbook:**
1. Monitor: `streamline_cluster_leader_changes_total` metric spikes
2. Alert: Leader change detected
3. Verify: `streamline-cli cluster status` shows new leader
4. Action: Investigate failed node, restart if possible

### Follower Failure
**Behavior:** Cluster continues operating with reduced redundancy.
**Expected recovery time:** Immediate (no interruption)
**Data impact:** None (leader continues accepting writes)

**Runbook:**
1. Monitor: `streamline_cluster_node_count` drops below expected
2. Alert: Node count < N threshold
3. Action: Restart failed node; it will catch up from Raft log
4. Verify: `streamline-cli cluster status` shows all nodes healthy

### Network Partition
**Behavior:** Minority partition becomes read-only; majority partition elects leader.
**Expected recovery time:** Automatic on partition heal
**Data impact:** None (Raft guarantees consistency)

**Runbook:**
1. Monitor: `streamline_cluster_partition_detected` alert
2. Verify: Majority side is serving traffic
3. Action: Fix network; nodes auto-rejoin
4. Post-incident: Check replication lag

### Split Brain Prevention
Streamline uses Raft quorum (N/2 + 1) to prevent split-brain:
- 3-node cluster: tolerates 1 failure
- 5-node cluster: tolerates 2 failures
- Minority partitions cannot elect a leader

## Rolling Upgrades

```bash
# 1. Check version compatibility
streamline-cli cluster version-check --target 0.3.0

# 2. Upgrade followers first, one at a time
# On each follower node:
systemctl stop streamline
# Install new version
systemctl start streamline
# Wait for node to rejoin and catch up
streamline-cli cluster wait-sync --node-id 2

# 3. Upgrade leader last (triggers leader election)
# On leader node:
streamline-cli cluster transfer-leadership --to 2
systemctl stop streamline
# Install new version
systemctl start streamline
```

## Rack Awareness

```toml
[cluster]
rack_id = "us-east-1a"
```

Replica placement ensures copies are spread across racks/zones.

## Autoscaling

The cluster autoscaler monitors load and can add/remove nodes:
```toml
[cluster.autoscaling]
enabled = true
min_nodes = 3
max_nodes = 9
scale_up_threshold_cpu = 80    # Scale up when CPU > 80%
scale_down_threshold_cpu = 20  # Scale down when CPU < 20%
cooldown_seconds = 300
```

## Monitoring

### Key Metrics
| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `streamline_cluster_leader_changes_total` | Leader elections | > 2/hour |
| `streamline_cluster_node_count` | Active nodes | < expected |
| `streamline_cluster_replication_lag_ms` | Replication delay | > 1000ms |
| `streamline_cluster_raft_commit_index` | Raft commit progress | Stalled > 30s |
| `streamline_cluster_partition_count` | Partitions per node | Imbalanced > 20% |
