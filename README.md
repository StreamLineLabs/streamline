# Streamline

[![Build Status](https://img.shields.io/github/actions/workflow/status/streamlinelabs/streamline/ci-full.yml?branch=main&style=flat-square)](https://github.com/streamlinelabs/streamline/actions)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange?style=flat-square)](https://www.rust-lang.org/)
[![Stability](https://img.shields.io/badge/stability-alpha-yellow?style=flat-square)](#project-status)
[![Documentation](https://img.shields.io/badge/docs-josedab.github.io%2Fstreamline-blue?style=flat-square)](https://josedab.github.io/streamline/)

**The Redis of Streaming** — A developer-first, operationally simple streaming solution that bridges the gap between enterprise platforms (Kafka, Redpanda) and simple messaging systems (Redis Pub/Sub).

- **🔌 Kafka Protocol Compatible** — Use existing Kafka clients without any code changes (50+ APIs)
- **📦 Single Binary** — One executable, no external dependencies, <50MB
- **⚡ Zero Configuration** — Run `./streamline` and it just works
- **🎯 Developer-First** — Optimized for local development and testing

## Quick Demo

<!-- TODO: Replace with animated terminal recording (asciinema/VHS) -->

```bash
# Start the server — that's it, no config needed
$ streamline --playground
[INFO] Streamline v0.2.0 — listening on 0.0.0.0:9092 (Kafka) / 0.0.0.0:9094 (HTTP)
[INFO] Playground mode: created topics [demo-events, demo-logs, demo-metrics, demo-orders]

# Create a topic with 3 partitions
$ streamline-cli topics create user-signups --partitions 3
✓ Created topic "user-signups" (3 partitions)

# Produce messages (supports JSON templates)
$ streamline-cli produce user-signups -m '{"user":"alice","plan":"pro"}'
ok: topic=user-signups partition=0 offset=0

# Consume messages in real time
$ streamline-cli consume user-signups --from-beginning
partition=0 offset=0 value={"user":"alice","plan":"pro"}

# Open the built-in TUI dashboard
$ streamline-cli top
# ┌─ Streamline Dashboard ──────────────────────────┐
# │ Topics: 5    Messages/s: 142    Uptime: 3m 12s  │
# │ user-signups  ▓▓▓▓▓░░░  3 partitions   1 msg   │
# │ demo-events   ▓▓▓▓▓▓▓░  1 partition  100 msgs   │
# └──────────────────────────────────────────────────┘
```

## See It in 60 Seconds

```bash
# 1. Install (no Rust required)
curl -fsSL https://raw.githubusercontent.com/streamlinelabs/streamline/main/scripts/install.sh | sh

# 2. Start the server with demo topics
streamline --playground

# 3. In another terminal — produce a message
streamline-cli produce demo -m '{"event":"signup","user":"alice"}'
# ok: topic=demo partition=0 offset=0

# 4. Consume it back
streamline-cli consume demo --from-beginning
# offset=0 value={"event":"signup","user":"alice"}
```

> Already have Kafka tools? They work unchanged:
> `kafka-console-producer --broker-list localhost:9092 --topic demo`

### More Ways to Start

```bash
# Docker (no install)
docker compose up -d && curl http://localhost:9094/health

# Full demo with produce/consume in Docker
docker compose -f docker-compose.demo.yml up

# From source (requires Rust 1.80+)
git clone https://github.com/streamlinelabs/streamline.git && cd streamline
cargo run -- --playground

# One-command quickstart (downloads binary, starts server, seeds demo data)
git clone https://github.com/streamlinelabs/streamline.git && cd streamline && ./scripts/quickstart.sh --download
```

### Quick Start with Docker

Run Streamline with a single command -- no install required:

```bash
docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:latest --playground
```

The `--playground` flag pre-creates demo topics (`demo-events`, `demo-logs`, `demo-metrics`, `demo-orders`) with sample data so you can start consuming immediately.

For the full demo environment with auto-seeded data and persistent storage, use `docker-compose.demo.yml` from [streamline-deploy](https://github.com/streamlinelabs/streamline-deploy):

```yaml
# docker-compose.demo.yml
services:
  streamline:
    image: ghcr.io/streamlinelabs/streamline:latest
    ports:
      - "9092:9092"   # Kafka protocol
      - "9094:9094"   # HTTP API
    command: ["--playground"]
    volumes:
      - streamline_data:/data
    restart: unless-stopped

volumes:
  streamline_data:
```

```bash
docker compose up -d
curl http://localhost:9094/health
```

### Try the CLI

```bash
streamline-cli topics create my-topic --partitions 3
streamline-cli produce my-topic -m "Hello, Streamline!"
streamline-cli consume my-topic --from-beginning
streamline-cli top                                      # TUI dashboard
```

> From source builds, run `make install` (recommended) or use `./target/release/streamline-cli`

## Local Development

- `./scripts/quickstart.sh` or `make quickstart` for a one-command demo
- `./scripts/quickstart-docker.sh` or `make docker-quickstart` for Docker-based demo
- `./scripts/dev-setup.sh --full` for a full dev environment bootstrap
- Devcontainer support via `.devcontainer/devcontainer.json`
- `./scripts/validate-60-seconds.sh` to verify first-message setup time
- Fast loops: `make quick-check` and `make test-lite`

See [Local Development](docs/LOCAL_DEVELOPMENT.md) for more workflows.

## Contribute in 5 Minutes

```bash
git clone https://github.com/streamlinelabs/streamline.git
cd streamline
./scripts/dev-setup.sh --minimal
make quick-check
make test-lite
```

## Installation

| Method | Command |
|--------|---------|
| **Homebrew** | `brew tap streamlinelabs/streamline && brew install streamline` |
| **Script** | `curl -fsSL https://raw.githubusercontent.com/streamlinelabs/streamline/main/scripts/install.sh | sh` |
| **Windows** | `iwr https://raw.githubusercontent.com/streamlinelabs/streamline/main/scripts/install.ps1 -UseBasicParsing | iex` |
| **Docker** | `docker run -d -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:latest` |
| **Binary** | Download from [GitHub Releases](https://github.com/streamlinelabs/streamline/releases) |
| **Source** | `make install` (or `cargo build --release` for `target/release/`) |

### Build Editions

| Edition | Command | Build Time* | Binary Size | Use Case |
|---------|---------|-------------|-------------|----------|
| **Lite** (default) | `cargo build --release` | ~5 min | ~8 MB | Development, testing, most deployments |
| **Full** | `cargo build --release --features full` | ~12 min | ~20 MB | Enterprise features (analytics, clustering, auth) |

*First build on a modern CPU. Subsequent incremental builds: ~10-30 seconds. Install [sccache](https://github.com/mozilla/sccache) for faster cross-branch rebuilds.

Individual features: `auth`, `clustering`, `telemetry`, `metrics`, `cloud-storage`, `schema-registry`, `encryption`, `analytics`, `iceberg`, `delta-lake`

## Features

### Core (Stable)
Persistent segment-based storage, log compaction, TLS/mTLS, Gzip/LZ4/Snappy/Zstd compression, zero-copy I/O, WebSocket gateway, Redis-like simple protocol, built-in TUI dashboard

### Enterprise (Feature-Gated)
SASL/OAuth authentication, ACL authorization, Raft-based clustering & replication, tiered storage (S3/Azure/GCS), Schema Registry (Avro/Protobuf/JSON Schema), client quotas, OpenTelemetry tracing, Prometheus metrics

### Experimental (Feature-Gated)
SQL analytics (DuckDB), Apache Iceberg & Delta Lake sinks, CDC (PostgreSQL/MySQL), stateful processing, time-series storage

## CLI Highlights

```bash
# Message templates for test data
streamline-cli produce my-topic --template '{"id":"{{uuid}}","ts":{{timestamp}}}' -n 1000

# Time-travel consume
streamline-cli consume my-topic --at "yesterday"
streamline-cli consume my-topic --last "5m"

# Grep, JSONPath extraction, sampling
streamline-cli consume my-topic --grep "error" -i
streamline-cli consume my-topic --jq "$.user.id"
streamline-cli consume my-topic --sample "10%"

# Diagnostics & migration
streamline-cli doctor
streamline-cli benchmark
streamline-cli migrate from-kafka --kafka-servers localhost:9092

# SQL queries (requires analytics feature)
streamline-cli query "SELECT * FROM streamline_topic('events') LIMIT 10"
```

See [CLI Reference](docs/CLI.md) for the complete CLI reference.

## Library Usage

```rust
use streamline::{EmbeddedStreamline, Result};
use bytes::Bytes;

fn main() -> Result<()> {
    let streamline = EmbeddedStreamline::in_memory()?;
    streamline.create_topic("events", 3)?;
    streamline.produce("events", 0, None, Bytes::from("hello world"))?;
    let records = streamline.consume("events", 0, 0, 100)?;
    Ok(())
}
```

## Language Quickstarts

> Streamline is Kafka protocol compatible — you can also use any standard Kafka client library
> ([examples](examples/external-clients/)).

- [Go SDK](sdks/go-sdk/README.md)
- [Python SDK](sdks/python-sdk/README.md) (Kafka protocol) | [Python HTTP SDK](sdks/python/README.md)
- [Node.js SDK](sdks/nodejs/README.md)
- [Java SDK](sdks/java/README.md)
- [.NET SDK](sdks/dotnet/README.md)
- [Rust SDK](sdks/rust/README.md)
- [Testcontainers](sdks/testcontainers-go/README.md)
- [All SDKs overview](sdks/README.md)

```python
import asyncio
from streamline import Streamline

async def main():
    async with Streamline("localhost:9092") as client:
        await client.produce("events", {"hello": "world"})

asyncio.run(main())
```

```typescript
import { Streamline } from 'streamline';

const client = new Streamline('localhost:9092');
await client.connect();
await client.produce('events', { hello: 'world' });
await client.close();
```

```go
config := streamline.DefaultConfig()
config.Brokers = []string{"localhost:9092"}
client, _ := streamline.NewClient(config)
client.Producer.Send(context.Background(), "events", nil, []byte("hello"))
```

## Examples

```bash
cargo run --example basic_producer
cargo run --example basic_consumer
cargo run --example topic_management
cargo run --example full_pipeline
```

Some examples require feature flags—check the example file header for details.

## Configuration

Zero configuration required by default. For customization:

```bash
./streamline --generate-config > streamline.toml    # Generate config template
./streamline -c streamline.toml                     # Use config file
./streamline --listen-addr 0.0.0.0:9092 --data-dir /data --log-level debug
```

Keep server and CLI aligned with a shared data dir:

```bash
export STREAMLINE_DATA_DIR=./data
streamline-cli profile add local --data-dir ./data
streamline-cli -P local topics list
```

See **[Configuration Reference](docs/CONFIGURATION.md)** for all options (TLS, clustering, auth, quotas, tracing, encryption, etc.).

## Documentation

| Resource | Description |
|----------|-------------|
| [Documentation Hub](DOCS.md) | Complete documentation index |
| [Getting Started](docs/GETTING_STARTED.md) | First 5/15/60 minute guide |
| [Local Development](docs/LOCAL_DEVELOPMENT.md) | Local setup, scripts, and workflows |
| [CLI Reference](docs/CLI.md) | Streamline CLI commands |
| [Configuration](docs/CONFIGURATION.md) | All server options |
| [Architecture](docs/ARCHITECTURE.md) | System design and components |
| [API Compatibility](docs/API_STABILITY.md) | Kafka API compatibility matrix |
| [Operations](docs/OPERATIONS.md) | Production operations guide |
| [Contributing](CONTRIBUTING.md) | Development setup and guidelines |

## Contributing

```bash
git clone https://github.com/streamlinelabs/streamline.git && cd streamline
make quick-check          # Fast compile check
make test-lite            # Lite test suite
make validate             # Full validation (fmt, clippy, test, doc)
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Project Status

> **⚠️ Alpha Software** — Streamline is under active development. Core features (Kafka protocol, storage, CLI) are stable. Clustering and enterprise features are beta.

**Recommended for:** Local development, testing, CI/CD pipelines, evaluating as a Kafka alternative.

**Not yet recommended for:** Production workloads with critical data, high-availability requirements.

See [API Stability](docs/API_STABILITY.md) for detailed compatibility matrices and [Benchmarks](docs/BENCHMARKS.md) for performance data.

## Community

We'd love to have you involved! Whether you have questions, ideas, or want to contribute — all are welcome.

- 💬 [GitHub Discussions](https://github.com/streamlinelabs/streamline/discussions) — Ask questions, share ideas, and connect with other users
- 🐛 [Issue Tracker](https://github.com/streamlinelabs/streamline/issues) — Report bugs or request features
- 🤝 [Contributing Guide](CONTRIBUTING.md) — Learn how to contribute to Streamline
- 📜 [Code of Conduct](CODE_OF_CONDUCT.md)

## License

Apache-2.0

---

Built with ❤️ by [Jose David Baena](https://github.com/josedab)
<!-- test: 451d957d -->
