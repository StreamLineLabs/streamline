# Streamline Jepsen Tests

Distributed systems correctness tests for Streamline using [Jepsen](https://jepsen.io/).

## Overview

This test suite validates Streamline's correctness guarantees under various failure conditions:

- **Linearizability**: Ensures operations appear to occur atomically
- **Durability**: Verifies committed data survives failures
- **Consistency**: Validates replication and ISR behavior

## Workloads

### Append Workload (`append`)
Tests linearizability of append operations to topics. Multiple clients append values to different keys, and the checker verifies that all appends are linearizable.

### Queue Workload (`queue`)
Tests FIFO ordering guarantees. Produces messages and consumes them, verifying proper ordering.

### Register Workload (`register`)
Tests single-key linearizability with read and write operations.

## Prerequisites

1. **Jepsen Control Node**: A machine with SSH access to test nodes
2. **Test Nodes**: 5 nodes (default) running Debian/Ubuntu
3. **Clojure**: Java 11+ and Leiningen

```bash
# Install Leiningen
curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod +x lein
sudo mv lein /usr/local/bin/
```

## Running Tests

### Basic Test

```bash
cd tests/jepsen
lein run test --nodes n1,n2,n3,n4,n5 \
    --username root \
    --workload append \
    --time-limit 120 \
    --rate 10
```

### Full Test with Nemesis

```bash
lein run test --nodes n1,n2,n3,n4,n5 \
    --username root \
    --workload append \
    --nemesis partition \
    --time-limit 300 \
    --rate 50
```

### Test Options

| Option | Description | Default |
|--------|-------------|---------|
| `--nodes` | Comma-separated list of test nodes | Required |
| `--username` | SSH username | root |
| `--workload` | Workload type (append, queue, register) | append |
| `--time-limit` | Test duration in seconds | 60 |
| `--rate` | Operations per second | 10 |
| `--partitions` | Topic partitions | 3 |
| `--replication-factor` | Replication factor | 3 |
| `--version` | Streamline version | latest |

## Fault Injection

The test suite can inject the following faults:

### Network Partitions
```bash
--nemesis partition
```
Creates network partitions isolating nodes.

### Process Kills
```bash
--nemesis kill
```
Kills Streamline processes.

### Process Pauses
```bash
--nemesis pause
```
Pauses processes with SIGSTOP.

### Combined Faults
```bash
--nemesis partition,kill,pause
```

## Test Results

Results are written to `store/` directory:

```
store/
└── streamline-append/
    └── 20241213T120000.000Z/
        ├── jepsen.log      # Detailed test log
        ├── history.edn     # Operation history
        ├── results.edn     # Test results
        └── timeline.html   # Visual timeline
```

## Interpreting Results

### Valid Result
```
Analysis complete.
Valid! ✓
```

### Invalid Result
```
Analysis complete.
Invalid! ✗

 :not-linearizable? true
 :bad-seq [...]
```

If tests fail, examine:
1. `history.edn` - Full operation history
2. `timeline.html` - Visual timeline of operations
3. `jepsen.log` - Detailed execution log

## Local Development

### Docker Compose Setup

For local testing without dedicated hardware:

```bash
# Start test cluster
docker-compose -f docker-compose.jepsen.yml up -d

# Run tests
lein run test --nodes node1,node2,node3 \
    --ssh-private-key ~/.ssh/id_rsa \
    --workload append
```

### Running Specific Tests

```bash
# Run unit tests
lein test

# Run specific namespace
lein test jepsen.streamline.core-test
```

## CI Integration

Add to GitHub Actions:

```yaml
jepsen-tests:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Set up JDK 11
      uses: actions/setup-java@v4
      with:
        java-version: '11'
        distribution: 'temurin'
    - name: Run Jepsen tests
      run: |
        cd tests/jepsen
        lein run test --nodes localhost \
          --workload append \
          --time-limit 60
```

## Known Issues

1. **Network partition tests** require `iptables` access
2. **Docker tests** may have different timing characteristics
3. **Slow disks** can affect linearizability timing

## References

- [Jepsen Documentation](https://jepsen.io/analyses)
- [Kafka Jepsen Analysis](https://jepsen.io/analyses/kafka)
- [Writing Jepsen Tests](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/index.md)
