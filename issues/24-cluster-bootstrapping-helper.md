# Add Cluster Bootstrapping Helper

## Priority: Nice to Have

## Summary

Create a helper tool or mode to simplify cluster bootstrapping. Currently, setting up a multi-node cluster requires manual configuration of each node, which is error-prone.

## Current State

- Each node requires manual `--node-id` and `--seed-nodes` configuration
- No validation that node IDs are unique
- No helper for generating cluster configuration
- No discovery mechanism for nodes

## Requirements

### Option 1: Bootstrap Command

```bash
# Generate configuration for a 3-node cluster
streamline bootstrap --nodes 3 --output ./cluster-config/

# Output:
# cluster-config/
#   node-1.env
#   node-2.env
#   node-3.env
#   docker-compose.yml (optional)
```

### Option 2: Bootstrap Mode

```bash
# First node starts in bootstrap mode
streamline --bootstrap --cluster-size 3 --advertised-addr node1:9092

# Other nodes join
streamline --join node1:9092 --advertised-addr node2:9092
streamline --join node1:9092 --advertised-addr node3:9092
```

### Option 3: Configuration Generator (CLI subcommand)

```bash
# Generate node configs interactively
streamline-cli cluster init

? How many nodes? 3
? Node 1 address: node1.example.com:9092
? Node 2 address: node2.example.com:9092
? Node 3 address: node3.example.com:9092
? Enable TLS? Yes
? Data directory: /var/lib/streamline

Generated configuration files:
  - node1.env
  - node2.env
  - node3.env
```

### Features

1. **Unique node ID assignment**
   - Auto-assign sequential IDs
   - Or use hostname hash for ID

2. **Seed node list generation**
   - Each node gets list of other nodes
   - Handle bootstrap node specially

3. **Configuration validation**
   - Warn about common misconfigurations
   - Validate network reachability (optional)

4. **Output formats**
   - Environment files (.env)
   - YAML configuration
   - Docker Compose
   - Kubernetes manifests

### Implementation Tasks

1. Design bootstrap UX (choose option)
2. Implement configuration generator
3. Add cluster validation checks
4. Generate environment files
5. Generate Docker Compose (optional)
6. Generate Kubernetes manifests (optional)
7. Add interactive prompts
8. Document bootstrap process

### Example Output (Environment Files)

```bash
# node-1.env
STREAMLINE_NODE_ID=1
STREAMLINE_LISTEN_ADDR=0.0.0.0:9092
STREAMLINE_ADVERTISED_ADDR=node1:9092
STREAMLINE_INTER_BROKER_ADDR=0.0.0.0:9093
STREAMLINE_SEED_NODES=node2:9093,node3:9093
STREAMLINE_DATA_DIR=/var/lib/streamline
STREAMLINE_REPLICATION_FACTOR=3

# node-2.env
STREAMLINE_NODE_ID=2
STREAMLINE_LISTEN_ADDR=0.0.0.0:9092
STREAMLINE_ADVERTISED_ADDR=node2:9092
STREAMLINE_INTER_BROKER_ADDR=0.0.0.0:9093
STREAMLINE_SEED_NODES=node1:9093,node3:9093
STREAMLINE_DATA_DIR=/var/lib/streamline
STREAMLINE_REPLICATION_FACTOR=3
```

### Acceptance Criteria

- [ ] Can bootstrap cluster with single command
- [ ] Generated configs are correct
- [ ] Node IDs are unique
- [ ] Seed nodes properly configured
- [ ] Documentation updated
- [ ] Works with Docker deployment

## Related Files

- `src/cli.rs` - CLI commands
- `src/config/mod.rs` - Configuration
- `docker-compose.yml` - Docker setup
- `src/cluster/mod.rs` - Cluster initialization

## Labels

`nice-to-have`, `developer-experience`, `cluster`
