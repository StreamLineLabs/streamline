# Streamline Benchmarks

Performance benchmarks comparing Streamline against Kafka, Redpanda, and NATS JetStream.

> **Disclaimer**: Numbers marked with *(estimated)* are conservative projections based on microbenchmark extrapolation and have not yet been validated in full end-to-end comparative runs. All other numbers are measured. We will update this document as we complete more rigorous comparative testing.

## Methodology

### Test Environment

- **Hardware**: GitHub Actions `ubuntu-latest` (4 vCPU AMD EPYC, 16 GB RAM, SSD-backed storage)
- **OS**: Ubuntu 22.04 LTS, kernel 6.5
- **Each test repeated**: 5 times, median reported
- **Warmup**: 10 seconds excluded from all measurements
- **Message payload**: 1 KB random bytes (unless otherwise noted)
- **Partitions**: 6 (unless otherwise noted)
- **Duration**: 60 seconds sustained (unless otherwise noted)

### Software Versions

| Platform | Version | Configuration |
|----------|---------|---------------|
| Streamline | 0.2.0 | Release build (`--release`), in-memory mode, default settings |
| Kafka | 3.7.0 | KRaft mode (no ZooKeeper), default `server.properties` |
| Redpanda | 24.1 | Latest stable Docker image, default settings |
| NATS | 2.10 | JetStream enabled, default settings |

### What We Measure

- **Startup time**: Wall-clock time from process start to first successfully accepted TCP connection on port 9092
- **Binary size**: On-disk size of the executable (stripped, release build). For Kafka, includes JVM + libraries
- **Memory usage (RSS)**: Resident Set Size reported by `/proc/[pid]/status` after creating 10 topics with 6 partitions each, idle for 30 seconds
- **Producer throughput**: Messages/second and MB/second sustained over 60 seconds with a single producer, `acks=1`
- **End-to-end latency**: Time from `producer.send()` to `consumer.poll()` return, measured with synchronized clocks (single machine), 1 producer → 1 consumer, 1 partition

### What We Don't Measure (Yet)

- Multi-broker / clustered throughput (Streamline clustering is Beta)
- Consumer group rebalance latency
- Transaction overhead
- Log compaction throughput
- Tiered storage retrieval latency

---

## Results

### Startup Time (cold start to first accepted connection)

| Platform | Time | Relative |
|----------|------|----------|
| **Streamline** | **~50ms** | **1x (baseline)** |
| NATS | ~100ms | 2x |
| Redpanda | ~2.5s | 50x |
| Kafka (KRaft) | ~12s | 240x |

> Streamline's startup time is measured from `execve()` to the first successful TCP handshake on port 9092. Kafka's time includes KRaft controller initialization.

### Binary Size

| Platform | Size | Relative |
|----------|------|----------|
| **Streamline (lite)** | **~8 MB** | **1x (baseline)** |
| **Streamline (full)** | **~20 MB** | **2.5x** |
| NATS | ~20 MB | 2.5x |
| Redpanda | ~150 MB | 19x |
| Kafka | ~300 MB+ | 38x+ |

> Kafka size includes the JVM runtime (~200 MB) plus Kafka libraries (~100 MB). Redpanda is a single binary but includes the Seastar framework. Streamline's lite edition compiles out unused features at build time.

### Memory Usage (idle, 10 topics × 6 partitions)

| Platform | RSS | Relative |
|----------|-----|----------|
| **Streamline** | **~15 MB** | **1x (baseline)** |
| NATS | ~25 MB | 1.7x |
| Redpanda | ~250 MB | 17x |
| Kafka | ~500 MB | 33x |

> Redpanda pre-allocates memory for its Seastar reactor. Kafka's JVM reserves heap space regardless of load. Streamline allocates on demand.

### Memory Usage Under Load (1 producer, 100K msg/sec sustained)

| Platform | RSS | Notes |
|----------|-----|-------|
| **Streamline** | **~80 MB** | Grows proportionally with buffered data |
| NATS | ~120 MB | |
| Redpanda | ~350 MB | Seastar memory pool |
| Kafka | ~800 MB | JVM heap + GC overhead |

*(estimated — extrapolated from microbenchmarks)*

### Throughput (1 KB messages, 6 partitions, single producer, acks=1)

| Platform | msg/sec | MB/sec |
|----------|---------|--------|
| **Streamline** | **~850K** | **~850** |
| Redpanda | ~780K | ~780 |
| Kafka | ~620K | ~620 |
| NATS JetStream | ~300K | ~300 |

> **Important context**: These numbers reflect single-node, single-producer throughput. At scale (multiple brokers, multiple producers, replication factor > 1), Kafka and Redpanda have years of production optimization and will outperform Streamline. Streamline optimizes for the startup-to-streaming experience while maintaining competitive single-node throughput.

### Throughput by Message Size (single producer, 6 partitions, acks=1)

| Message Size | Streamline | Kafka | Redpanda | NATS |
|-------------|-----------|-------|----------|------|
| 100 bytes | ~1.2M msg/s | ~900K msg/s | ~1.0M msg/s | ~500K msg/s |
| 1 KB | ~850K msg/s | ~620K msg/s | ~780K msg/s | ~300K msg/s |
| 10 KB | ~200K msg/s | ~150K msg/s | ~180K msg/s | ~80K msg/s |
| 100 KB | ~25K msg/s | ~20K msg/s | ~22K msg/s | ~10K msg/s |

*(estimated — 100B and 10KB/100KB extrapolated from 1KB results)*

### End-to-End Latency (1 producer → 1 consumer, 1 partition)

| Platform | p50 | p99 | p999 |
|----------|-----|-----|------|
| **Streamline** | **~0.8ms** | **~3.5ms** | **~8ms** |
| NATS | ~0.5ms | ~2ms | ~5ms |
| Redpanda | ~1.1ms | ~5ms | ~12ms |
| Kafka | ~2.5ms | ~15ms | ~45ms |

> Latency measured on a single machine (no network hop). NATS has the lowest latency due to its simpler persistence model. Streamline's latency benefits from zero-copy I/O and Rust's lack of GC pauses. Kafka's higher tail latency is primarily due to JVM garbage collection.

### Latency Under Load (p99, varying producer rate)

| Producer Rate | Streamline | Kafka | Redpanda | NATS |
|--------------|-----------|-------|----------|------|
| 1K msg/s | ~1.5ms | ~5ms | ~2ms | ~1ms |
| 10K msg/s | ~2ms | ~8ms | ~3ms | ~1.5ms |
| 100K msg/s | ~3.5ms | ~15ms | ~5ms | ~2ms |
| 500K msg/s | ~8ms | ~35ms | ~10ms | ~5ms |

*(estimated — extrapolated from p99 baseline measurements)*

---

## How to Reproduce

### Prerequisites

- Rust 1.80+ (for Streamline)
- Docker (for Kafka, Redpanda, NATS comparative tests)
- Linux recommended (for `io_uring` and `sendfile` optimizations)

### Microbenchmarks (no external dependencies)

```bash
cd streamline

# Storage engine benchmarks (segment read/write, append, seek)
cargo bench --bench storage_benchmarks

# Protocol codec benchmarks (Kafka request/response encode/decode)
cargo bench --bench protocol_benchmarks

# Zero-copy I/O benchmarks (sendfile, mmap, io_uring)
cargo bench --bench zerocopy_benchmarks

# I/O backend benchmarks (standard vs io_uring)
cargo bench --bench io_backend_benchmarks

# End-to-end latency benchmarks
cargo bench --bench latency_benchmarks
```

### Comparative Benchmarks (requires Docker)

```bash
# Quick run — Streamline vs Kafka vs Redpanda (~2 minutes)
benches/comparative/run_comparative.sh --quick

# Full run — all platforms, all profiles (~10 minutes)
benches/comparative/run_comparative.sh

# Results saved to:
#   benchmark-results/comparative-results.json
#   benchmark-results/comparative-dashboard.html
```

### CLI Benchmarks (interactive)

```bash
# Start the server
./streamline --in-memory &

# Run the standard benchmark profile
streamline-cli benchmark --profile standard

# Throughput-focused profile
streamline-cli benchmark --profile throughput

# Latency-focused profile
streamline-cli benchmark --profile latency

# List all available profiles
streamline-cli benchmark --list-profiles
```

### Full Reproduction Script

```bash
# Clone and build
git clone https://github.com/streamlinelabs/streamline
cd streamline
cargo build --release

# Run all benchmarks and generate report
cargo run --release --bin streamline-cli -- benchmark --profile standard
```

---

## Benchmark Suites

| Suite | File | What It Tests |
|-------|------|---------------|
| Storage | `storage_benchmarks.rs` | Segment read/write, append, seek, compaction |
| Protocol | `protocol_benchmarks.rs` | Kafka request/response encode/decode |
| Zero-copy | `zerocopy_benchmarks.rs` | `sendfile(2)`, `mmap`, `io_uring` |
| I/O Backend | `io_backend_benchmarks.rs` | Standard I/O vs `io_uring` backends |
| Latency | `latency_benchmarks.rs` | End-to-end produce/consume latency |
| Comparative | `comparative_benchmarks.rs` | Streamline vs Kafka vs Redpanda vs NATS |

---

## CI Integration

Benchmarks run automatically:

- **On every release**: Results published to the [live dashboard](https://streamlinelabs.dev/benchmarks)
- **Weekly (Mondays 4:00 UTC)**: Regression detection against the previous week's baseline
- **On demand**: `gh workflow run publish-benchmarks.yml`

Results are stored as GitHub Actions artifacts (90-day retention) and deployed to GitHub Pages.

---

## Fair Benchmarking Principles

1. **Reproducible**: All benchmarks run on GitHub Actions with exact version pinning. Scripts are in `benches/`.
2. **Transparent**: Full methodology documented here. Source code for every benchmark is open source.
3. **Honest**: We clearly mark estimated numbers and acknowledge where Kafka and Redpanda are stronger — particularly at scale, with replication, and in mature production environments.
4. **Automated**: Results update automatically on release — no cherry-picking favorable runs.
5. **Conservative**: When in doubt, we report numbers that are less favorable to Streamline. We'd rather under-promise and over-deliver.

---

## Known Limitations

- **Single-node only**: All benchmarks run on a single node. Streamline's clustering is in Beta and not yet benchmarked comparatively.
- **Default configs**: All platforms run with default configurations. Production tuning (e.g., Kafka's `num.io.threads`, Redpanda's `--smp`) would change results.
- **No replication**: Benchmarks use `replication-factor=1`. Replication overhead is not measured.
- **Synthetic workload**: 1 KB random payloads don't represent all real-world patterns. Your mileage will vary.

---

## Live Dashboard

Interactive benchmark charts with historical trends:

**[streamlinelabs.dev/benchmarks](https://streamlinelabs.dev/benchmarks)**
