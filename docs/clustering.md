# Clustering Guide

Streamline supports multi-node clustering using the **Raft consensus protocol** (via [openraft](https://github.com/datafuselabs/openraft)). Clustering provides high availability, automatic leader election, and data replication across nodes.

> **Status**: Production-ready. Clustering is gated behind the `clustering` Cargo feature flag.

## Architecture Overview

### Raft Consensus

Streamline uses Raft for **cluster metadata consensus** — not for data replication of topic messages. The Raft state machine manages:

- **Broker registration** — which nodes are in the cluster and their states
- **Topic assignments** — partition-to-broker mapping, replica sets, ISR lists
- **Leader election** — which broker is the partition leader
- **Cluster controller** — the Raft leader acts as the cluster controller

Data replication (actual topic messages) is handled by a separate replication layer that uses the Raft-managed metadata to know which partitions to replicate and where.

### Node Roles

| Role | Description |
|------|-------------|
| **Leader** (Controller) | Manages cluster metadata, handles membership changes, coordinates topic creation |
| **Follower** (Voter) | Replicates Raft log, participates in elections, can become leader |
| **Learner** | Receives Raft log but cannot vote; used during node provisioning |

### Communication

- **Port 9092** — Kafka protocol (client connections)
- **Port 9093** — Inter-broker RPC (Raft consensus + data replication)
- **Port 9094** — HTTP API (health checks, metrics, cluster management)

Inter-broker communication can be secured with mutual TLS (mTLS).

## Setup Guide

### 3-Node Cluster

#### Node 1 (Bootstrap Node)

```bash
streamline \
  --cluster-node-id 1 \
  --advertised-addr 192.168.1.1:9092 \
  --inter-broker-addr 0.0.0.0:9093 \
  --data-dir /var/lib/streamline/node1
```

The first node bootstraps automatically when no seed nodes are configured.

#### Node 2

```bash
streamline \
  --cluster-node-id 2 \
  --advertised-addr 192.168.1.2:9092 \
  --inter-broker-addr 0.0.0.0:9093 \
  --seed-nodes 192.168.1.1:9093 \
  --data-dir /var/lib/streamline/node2
```

#### Node 3

```bash
streamline \
  --cluster-node-id 3 \
  --advertised-addr 192.168.1.3:9092 \
  --inter-broker-addr 0.0.0.0:9093 \
  --seed-nodes 192.168.1.1:9093 \
  --data-dir /var/lib/streamline/node3
```

Nodes 2 and 3 join the cluster through the seed node (Node 1). If Node 1 is not the Raft leader, the join request is automatically forwarded to the current leader.

### Docker Compose (3-Node)

```yaml
version: '3.8'
services:
  streamline-1:
    image: streamlinelabs/streamline:latest
    command: >
      --cluster-node-id 1
      --advertised-addr streamline-1:9092
      --inter-broker-addr 0.0.0.0:9093
    ports:
      - "9092:9092"
      - "9094:9094"

  streamline-2:
    image: streamlinelabs/streamline:latest
    command: >
      --cluster-node-id 2
      --advertised-addr streamline-2:9092
      --inter-broker-addr 0.0.0.0:9093
      --seed-nodes streamline-1:9093
    ports:
      - "9192:9092"
      - "9194:9094"

  streamline-3:
    image: streamlinelabs/streamline:latest
    command: >
      --cluster-node-id 3
      --advertised-addr streamline-3:9092
      --inter-broker-addr 0.0.0.0:9093
      --seed-nodes streamline-1:9093
    ports:
      - "9292:9092"
      - "9294:9094"
```

## Configuration Reference

### Core Clustering Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `node_id` | 1 | Unique node ID within the cluster (must be > 0) |
| `advertised_addr` | `127.0.0.1:9092` | Address returned to clients in metadata responses |
| `inter_broker_addr` | `0.0.0.0:9093` | Address for inter-broker Raft and replication traffic |
| `seed_nodes` | `[]` | Inter-broker addresses of existing cluster members for joining |

### Replication Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_replication_factor` | 1 | Default number of replicas for new topics |
| `min_insync_replicas` | 1 | Minimum ISR size required for writes with `acks=all` |
| `unclean_leader_election` | `false` | Allow non-ISR replicas to become leader (risks data loss) |

### Raft Consensus Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `heartbeat_interval_ms` | 500 | Raft heartbeat interval. Lower = faster failure detection, more network traffic |
| `election_timeout_min_ms` | 1000 | Minimum election timeout. Must be > heartbeat interval |
| `election_timeout_max_ms` | 2000 | Maximum election timeout. Randomized between min and max |
| `session_timeout_ms` | 10000 | Time before a node is considered dead |

### Snapshot & Log Compaction

| Parameter | Default | Description |
|-----------|---------|-------------|
| `snapshot_interval_logs` | 1000 | Number of Raft log entries between automatic snapshots |
| `max_logs_in_snapshot` | 1000 | Maximum log entries retained after snapshot for replication catch-up |
| `purge_batch_size` | 100 | Number of log entries purged per batch during compaction |
| `max_payload_entries` | 100 | Maximum entries per Raft append request |

**Tuning guidance:**
- For clusters with high metadata change rates (frequent topic creation/deletion), lower `snapshot_interval_logs` to 500.
- For clusters with slow joiners, increase `max_logs_in_snapshot` to give new nodes more time to catch up from logs before needing a full snapshot transfer.
- `purge_batch_size` controls memory pressure during compaction; larger batches are faster but use more memory.

### Inter-Broker TLS

| Parameter | Default | Description |
|-----------|---------|-------------|
| `inter_broker_tls.enabled` | `false` | Enable mTLS for inter-broker communication |
| `inter_broker_tls.cert_path` | — | Path to this node's TLS certificate (PEM) |
| `inter_broker_tls.key_path` | — | Path to this node's private key (PEM) |
| `inter_broker_tls.ca_cert_path` | — | Path to CA certificate for peer verification |
| `inter_broker_tls.verify_peer` | `true` | Whether to verify peer certificates |
| `inter_broker_tls.min_version` | `"1.2"` | Minimum TLS version (`"1.2"` or `"1.3"`) |

### Rack-Aware Placement

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rack_id` | — | Rack/AZ identifier for this node |
| `rack_aware_assignment` | `true` | Spread replicas across racks when possible |

## HTTP API Endpoints

### Cluster Health

**GET /api/v1/cluster/status**

Returns the Raft state, leader information, log indices, and overall health.

```json
{
  "node_state": "Leader",
  "node_id": 1,
  "leader_id": 1,
  "committed_log_index": 42,
  "applied_log_index": 42,
  "current_term": 3,
  "voters": [1, 2, 3],
  "learners": [],
  "healthy": true,
  "health_reason": "Node is a healthy voting member with an elected leader"
}
```

### Cluster Members

**GET /api/v1/cluster/members**

```json
{
  "members": [
    {
      "node_id": 1,
      "raft_addr": "192.168.1.1:9093",
      "role": "voter",
      "is_leader": true,
      "broker_state": "Running",
      "advertised_addr": "192.168.1.1:9092"
    }
  ],
  "total": 3,
  "leader_id": 1
}
```

**POST /api/v1/cluster/members** — Add a new member (must be sent to the leader).

```json
{
  "node_id": 4,
  "raft_addr": "192.168.1.4:9093",
  "promote_to_voter": true
}
```

**DELETE /api/v1/cluster/members/{node_id}** — Remove a member (must be sent to the leader).

## Failure Scenarios and Recovery

### Leader Failure

1. Followers detect missing heartbeats after `election_timeout_min_ms`–`election_timeout_max_ms`.
2. A follower starts an election by incrementing its term and requesting votes.
3. If it receives a majority of votes, it becomes the new leader.
4. **Recovery time**: Typically `election_timeout_max_ms` (2 seconds by default).
5. **Data impact**: No data loss. Committed entries are durable on a majority of nodes.

### Follower Failure

1. The leader detects the follower is unreachable after `session_timeout_ms`.
2. The follower is removed from ISR lists for its partitions.
3. Writes continue as long as `min_insync_replicas` is satisfied.
4. When the follower recovers, it catches up from the leader's log or snapshot.
5. **Data impact**: None, as long as `min_insync_replicas` other nodes are alive.

### Network Partition

1. The minority side cannot elect a leader (needs majority).
2. The majority side continues operating normally.
3. When the partition heals, minority nodes rejoin and catch up.
4. **Prevention**: Use an odd number of nodes (3 or 5) across at least 2 failure domains.

### Graceful Shutdown

Streamline performs a graceful leadership transfer on shutdown:

1. If the shutting-down node is the leader, it sends a final heartbeat to ensure followers are caught up.
2. The node deregisters itself from the cluster (marks as `ShuttingDown`).
3. The replication manager and Raft engine shut down with timeouts.
4. Remaining nodes elect a new leader automatically.

### Full Cluster Restart

If all nodes go down simultaneously:

1. Start any single node — it will reload its Raft state from disk.
2. If that node was the last leader, it can immediately resume.
3. Start additional nodes — they will rejoin using their persisted state.
4. **Important**: Do not change `node_id` values between restarts.

### Adding/Removing Nodes

**Adding a node:**
```bash
# Option 1: Start the node with --seed-nodes pointing to an existing member
streamline --cluster-node-id 4 --seed-nodes 192.168.1.1:9093 ...

# Option 2: Use the HTTP API from the leader
curl -X POST http://leader:9094/api/v1/cluster/members \
  -H 'Content-Type: application/json' \
  -d '{"node_id": 4, "raft_addr": "192.168.1.4:9093"}'
```

**Removing a node:**
```bash
# Graceful removal via API (must target the leader)
curl -X DELETE http://leader:9094/api/v1/cluster/members/4
```

## Monitoring Guide

### Key Metrics to Watch

| Metric / Endpoint | What It Tells You | Alert Threshold |
|---|---|---|
| `GET /api/v1/cluster/status` → `healthy` | Overall node health | Alert if `false` for > 30s |
| `GET /api/v1/cluster/status` → `leader_id` | Whether a leader exists | Alert if `null` for > 10s |
| `GET /api/v1/cluster/status` → `node_state` | Raft role of this node | Alert if `Candidate` for > 30s (stuck election) |
| `committed_log_index` vs `applied_log_index` | State machine lag | Alert if difference > 100 |
| `GET /api/v1/cluster/members` → `total` | Cluster size | Alert if < expected quorum size |
| `GET /health/ready` | Readiness probe | Use for K8s readiness checks |
| `GET /health/live` | Liveness probe | Use for K8s liveness checks |

### Health Check Integration

For Kubernetes deployments:

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9094
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /api/v1/cluster/status
    port: 9094
  initialDelaySeconds: 5
  periodSeconds: 5
```

Parse the readiness response and check `healthy == true` before routing traffic.

### Operational Runbook

1. **No leader elected**: Check network connectivity between nodes. Verify at least a majority of nodes are running. Check for clock skew.
2. **Node stuck as Candidate**: The node may be partitioned. Check firewall rules on port 9093. Verify `election_timeout_max_ms` is reasonable.
3. **High log index gap**: The state machine is falling behind. Check disk I/O and CPU on the affected node.
4. **Frequent leader changes**: Increase `election_timeout_min_ms` and `heartbeat_interval_ms`. Check for network instability.
5. **Node cannot join**: Ensure the seed node is the leader or can forward. Check that `node_id` is unique and > 0.
