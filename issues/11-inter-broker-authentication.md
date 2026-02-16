# Implement Inter-Broker Authentication

## Priority: Critical

## Summary

Cluster nodes currently communicate without authentication. Any node can join the cluster and impersonate another node, creating a significant security vulnerability in multi-node deployments.

## Current State

- `src/cluster/mod.rs` - Nodes connect via Raft protocol without authentication
- `src/replication/` - Replication traffic is unauthenticated
- Inter-broker communication uses plaintext RPC
- No validation that connecting nodes are legitimate cluster members

## Security Risk

- **Spoofing**: Malicious node can join cluster and receive replicated data
- **Data exfiltration**: Attacker node can request partition data
- **Cluster disruption**: Rogue node can participate in leader elections

## Requirements

### Authentication Options

1. **mTLS for inter-broker communication** (Recommended)
   - Each node has a certificate signed by cluster CA
   - Nodes verify peer certificates before accepting connections
   - Reuse existing TLS infrastructure from client connections

2. **Shared secret authentication**
   - Cluster-wide secret configured on all nodes
   - HMAC-based authentication of RPC messages
   - Simpler but less secure than mTLS

### Implementation Tasks

1. Add `--inter-broker-tls-enabled` configuration option
2. Add `--inter-broker-tls-cert` and `--inter-broker-tls-key` options
3. Modify Raft transport layer to use TLS
4. Modify replication fetcher/sender to use TLS
5. Add node identity validation (verify node ID matches certificate CN/SAN)
6. Log authentication failures with source IP

### Configuration

```bash
# Inter-broker TLS settings
--inter-broker-tls-enabled=true
--inter-broker-tls-cert=/path/to/broker.crt
--inter-broker-tls-key=/path/to/broker.key
--inter-broker-tls-ca=/path/to/cluster-ca.crt
```

### Acceptance Criteria

- [ ] Inter-broker connections require mutual TLS when enabled
- [ ] Unauthenticated nodes cannot join cluster
- [ ] Authentication failures are logged with details
- [ ] Existing single-node deployments unaffected
- [ ] Documentation updated with cluster security setup

## Related Files

- `src/cluster/mod.rs` - Cluster membership
- `src/cluster/raft.rs` - Raft consensus transport
- `src/replication/fetcher.rs` - Follower replication
- `src/replication/sender.rs` - Leader replication
- `src/config/mod.rs` - Configuration options

## Labels

`critical`, `security`, `cluster`, `production-readiness`
