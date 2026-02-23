# Development Guide

Getting started with Streamline development in under 5 minutes.

## Prerequisites

- **Rust 1.80+**: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Git**: For cloning the repository

Optional:
- **Docker**: For integration tests and comparative benchmarks
- **cargo-fuzz**: For fuzz testing (`cargo install cargo-fuzz`)
- **cargo-nextest**: For faster test execution (`cargo install cargo-nextest`)

## Quick Start

```bash
# Clone
git clone https://github.com/streamlinelabs/streamline
cd streamline

# Build (Lite edition, ~2 minutes first build)
cargo build

# Run tests
cargo test

# Start the server
cargo run -- --in-memory

# In another terminal, verify it works
curl http://localhost:9094/health
```

## Build Editions

| Command | Edition | Size | Time | Features |
|---------|---------|------|------|----------|
| `cargo build` | Lite | ~8 MB | ~2 min | Core streaming + compression |
| `cargo build --features full` | Full | ~20 MB | ~5 min | Everything |
| `cargo build --release` | Release | ~8 MB | ~5 min | Optimized |

## Common Development Tasks

```bash
# Format code
cargo fmt

# Lint
cargo clippy --all-targets --all-features

# Run specific test
cargo test test_name

# Run benchmarks
cargo bench --bench storage_benchmarks

# Check all features compile
cargo check --all-features

# Full validation (format + clippy + test + audit)
make validate

# Quick compile check (~30s)
make quick-check

# Run fuzz tests (60s each)
make fuzz

# Check v1.0 readiness
make v1-readiness
```

## Project Structure

```
src/
├── main.rs           # Server binary
├── cli.rs            # CLI binary
├── lib.rs            # Library root (63 modules)
├── embedded.rs       # Embeddable library API
├── error.rs          # Error types (thiserror)
├── config/           # Configuration
├── server/           # HTTP + WebSocket (46 API modules)
├── protocol/         # Kafka wire protocol
├── storage/          # Segment-based storage (38 files)
├── consumer/         # Consumer groups
├── connect/          # Connector framework (30 catalog, 16 implemented)
├── streamql/         # SQL-on-streams (17 files)
├── gateway/          # MQTT, AMQP, gRPC adapters
├── edge/             # Edge runtime (15 files)
├── observability/    # Metrics, tracing, anomaly detection
└── plugin/           # Extensions SDK

tests/                # 58 integration test files
benches/              # 11 benchmark suites
fuzz/                 # 5 fuzz targets + corpus seeds
examples/             # 7 Rust + 6 shell examples
include/              # C header (streamline.h, 294 lines)
docs/                 # 13+ documentation files
```

## Coding Standards

### Linting Rules (17 clippy rules enforced)

| Rule | Level | Purpose |
|------|-------|---------|
| `unwrap_used` | **deny** | No unwrap — use `?` or `.ok_or()` |
| `todo` | **deny** | No incomplete code |
| `unimplemented` | **deny** | Use proper error types |
| `dbg_macro` | **deny** | No debug macros |
| `panic` | warn | Prefer Result over panic |
| `expect_used` | warn | Discourage expect |
| `print_stdout/stderr` | warn | Use `tracing::info!` |

Files with legacy violations are quarantined with `#![allow(...)]` — see
[UNWRAP_MIGRATION.md](docs/UNWRAP_MIGRATION.md) for the fix guide.

### Error Handling

```rust
// Use the crate's Result type
use crate::error::{Result, StreamlineError};

// Return errors, don't panic
pub fn my_function() -> Result<()> {
    let value = some_operation().map_err(|e| StreamlineError::storage(e))?;
    Ok(())
}
```

### Testing

```bash
# Unit tests (inline in each module)
cargo test

# Integration tests
cargo test --test integration_test

# Property-based tests
cargo test --test property_test

# Cross-SDK compatibility
cargo test --test cross_sdk_test

# Fuzz testing (requires cargo-fuzz)
cargo fuzz run fuzz_kafka_requests -- -max_total_time=60
```

## Key Documentation

| Document | Purpose |
|----------|---------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System overview, module map, data flows |
| [API_STABILITY.md](docs/API_STABILITY.md) | Module stability tiers, Kafka compat matrix |
| [UNWRAP_MIGRATION.md](docs/UNWRAP_MIGRATION.md) | How to fix quarantined unwrap() calls |
| [STORAGE_REFACTORING.md](docs/STORAGE_REFACTORING.md) | Plan for decomposing large storage files |
| [DEAD_CODE_AUDIT.md](docs/DEAD_CODE_AUDIT.md) | Tracking unused code annotations |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and fixes |
| [KAFKA_MIGRATION.md](docs/KAFKA_MIGRATION.md) | Migrating from Kafka |
| [BENCHMARKS.md](docs/BENCHMARKS.md) | Performance methodology and results |

## IDE Setup

### VS Code
Install `rust-analyzer` extension. The workspace should auto-detect.

### IntelliJ / CLion
Install the Rust plugin. Open the project root.

### Recommended `.cargo/config.toml`
```toml
[build]
# Use all CPU cores
jobs = 0

[target.x86_64-unknown-linux-gnu]
# Enable mold linker for faster linking (Linux)
# linker = "clang"
# rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```
