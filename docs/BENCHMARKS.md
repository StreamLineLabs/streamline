# Benchmarks

Streamline performance benchmarks — methodology, results, and reproduction instructions.

## Quick Comparison

| Metric | Streamline | Kafka 3.7 | Redpanda |
|--------|-----------|-----------|----------|
| Binary size | ~8 MB | ~300 MB* | ~150 MB |
| Startup time | ~45 ms | ~12 sec | ~2.5 sec |
| Memory (idle) | ~15 MB | ~500 MB | ~200 MB |
| Dependencies | None | JVM + ZK/KRaft | None |
| Producer throughput | ~850K msg/s | ~620K msg/s | ~780K msg/s |
| E2E latency (p50) | ~0.8 ms | ~2.5 ms | ~1.1 ms |
| E2E latency (p99) | ~3.5 ms | ~15.0 ms | ~5.0 ms |

*Including JVM + ZooKeeper

## Methodology

### Environment
- **Runner**: GitHub Actions ubuntu-latest (4 vCPU, 16GB RAM, SSD)
- **Message size**: 1 KB
- **Partitions**: 6
- **Duration**: 60 seconds (10s warmup excluded)
- **Kafka version**: 3.7.0 (KRaft mode, no ZooKeeper)
- **Redpanda**: Latest stable Docker image
- **Streamline**: Release build, in-memory mode

### What We Measure
- **Producer throughput**: Messages/second and MB/second sustained over 60s
- **E2E latency**: Time from producer send to consumer receive (p50, p99, p999)
- **Startup time**: Time from process start to first successful health check
- **Memory usage**: RSS at idle (no topics) and under load

### What We Don't Measure (Yet)
- Multi-broker throughput (clustering is Beta)
- Consumer group rebalance latency
- Transaction overhead
- Compaction throughput

## Running Benchmarks Locally

### Microbenchmarks (no external dependencies)

```bash
# Storage engine benchmarks
cargo bench --bench storage_benchmarks

# Protocol codec benchmarks
cargo bench --bench protocol_benchmarks

# Zero-copy I/O benchmarks
cargo bench --bench zerocopy_benchmarks

# I/O backend benchmarks
cargo bench --bench io_backend_benchmarks

# Latency benchmarks
cargo bench --bench latency_benchmarks
```

### Comparative Benchmarks (requires Docker)

```bash
# Quick run (~2 minutes)
benches/comparative/run_comparative.sh --quick

# Full run (~10 minutes)
benches/comparative/run_comparative.sh

# Results saved to benchmark-results/comparative-results.json
# Dashboard generated at benchmark-results/comparative-dashboard.html
```

### CLI Benchmarks

```bash
# Start server first
./streamline --in-memory &

# Throughput profile
streamline-cli benchmark --profile throughput

# Latency profile
streamline-cli benchmark --profile latency

# List all profiles
streamline-cli benchmark --list-profiles
```

## CI Integration

Benchmarks run automatically:
- **On every release**: Results published to the [live dashboard](https://streamlinelabs.dev/benchmarks)
- **Weekly (Mondays 4am UTC)**: Regression detection
- **On demand**: `gh workflow run publish-benchmarks.yml`

Results are stored as GitHub Actions artifacts (90-day retention) and
deployed to GitHub Pages.

## Benchmark Suites

| Suite | File | What It Tests |
|-------|------|--------------|
| Storage | `storage_benchmarks.rs` | Segment read/write, append, seek |
| Protocol | `protocol_benchmarks.rs` | Kafka request/response encode/decode |
| Zero-copy | `zerocopy_benchmarks.rs` | sendfile, mmap, io_uring |
| I/O Backend | `io_backend_benchmarks.rs` | Standard vs io_uring backends |
| Latency | `latency_benchmarks.rs` | End-to-end produce/consume latency |
| Comparative | `comparative_benchmarks.rs` | Streamline vs Kafka vs Redpanda |

## Fair Benchmarking Principles

1. **Reproducible**: All benchmarks run on GitHub Actions with exact version pinning
2. **Transparent**: Full methodology documented, source code available
3. **Honest**: We acknowledge where Kafka/Redpanda are stronger (scale, ecosystem)
4. **Automated**: Results update automatically — no cherry-picking runs

## Live Dashboard

Interactive benchmark dashboard with charts:
https://streamlinelabs.dev/benchmarks
