# Contributing to Streamline

Thank you for your interest in contributing to Streamline — "The Redis of Streaming"! This guide will help you get started, whether you're fixing a typo or building a new storage backend.

## Quick Links

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Issue Tracker](https://github.com/streamlinelabs/streamline/issues)
- [Discord Community](https://discord.gg/streamline)
- [Local Development Guide](docs/LOCAL_DEVELOPMENT.md)
- [API Stability & Deprecation Policy](#api-stability)

---

## Getting Started

### Prerequisites

- **Rust 1.80+** — install via `rustup update stable`
- **Git**
- `pkg-config` (Linux: `apt-get install pkg-config`, macOS: `brew install pkg-config`)
- **Docker** (optional, for integration tests and container workflows)
- **Node.js 18+** (only required for the documentation website)

**Optional but recommended:**

- `cargo-nextest` — faster, clearer test output
- `sccache` — caches compilation artifacts across branch switches (`cargo install sccache`)
- `cargo-watch` — file-watching dev loop (`cargo install cargo-watch`)
- `cargo-machete` — detect unused dependencies (`cargo install cargo-machete`)
- `pre-commit` — git hooks for automated formatting checks

### Building from Source

```bash
git clone https://github.com/streamlinelabs/streamline.git
cd streamline
cargo build                    # Lite edition (core streaming, TLS, compression)
cargo build --features full    # Full edition (+ auth, clustering, telemetry, cloud storage)
cargo test                     # Run the test suite
```

### Running Locally

```bash
cargo run                      # Start server on localhost:9092
cargo run -- --playground      # Start with demo topics and sample data
RUST_LOG=debug cargo run       # Start with debug logging
```

The server exposes two ports:
- **9092** — Kafka wire protocol (producer/consumer traffic)
- **9094** — HTTP API (health checks, metrics, management)

### Full Development Setup

```bash
# Bootstrap a complete dev environment (installs tools, git hooks, etc.)
./scripts/dev-setup.sh --full

# Install binaries for local CLI usage
make install

# Fast feedback cycle
make quick-check               # fmt + clippy check
make test-lite                 # tests with default features only
```

For devcontainer and additional workflows, see [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md).

### Build Editions

| Edition | Command | Features Included |
|---------|---------|-------------------|
| **Lite** (default) | `cargo build` | Core streaming, TLS, compression |
| **Full** | `cargo build --features full` | + Auth, clustering, telemetry, cloud storage |

### Cargo Aliases

The project provides shortcuts in `.cargo/config.toml`:

| Alias | Expands To | Purpose |
|-------|-----------|---------|
| `cargo c` | `cargo check --all-features` | Quick compilation check |
| `cargo dev` | `cargo watch -x 'check --all-features'` | Watch mode |
| `cargo lint` | `cargo clippy --all-targets --all-features -- -D warnings` | Lint with strict warnings |
| `cargo t` | `cargo test --all-features` | Run all tests |
| `cargo lite` | `cargo build` | Build lite edition |
| `cargo full` | `cargo build --features full` | Build full edition |
| `cargo rel` | `cargo build --release` | Release build |
| `cargo docs` | `cargo doc --no-deps --all-features --open` | Build and open API docs |
| `cargo play` | `cargo run --bin streamline -- --playground` | Server in playground mode |
| `cargo top` | `cargo run --bin streamline-cli -- top` | TUI dashboard |
| `cargo unused` | `cargo machete` | Check unused deps |

### Build Caching with sccache

For faster rebuilds across branch switches, install [sccache](https://github.com/mozilla/sccache):

```bash
cargo install sccache
```

Then uncomment the `rustc-wrapper` line in `.cargo/config.toml`:

```toml
rustc-wrapper = "sccache"
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────┐
│                 Streamline Server                  │
├──────────────────────────────────────────────────┤
│ server/ (75 modules)  │  protocol/ (51 files)     │
│   REST API (411 routes) │   Kafka wire protocol   │
├──────────────────────────────────────────────────┤
│ storage/ (58 files)   │  streamql/ (22 files)     │
│   Segments, tiering    │   SQL engine, federation  │
├──────────────────────────────────────────────────┤
│ ai/ (24 files)  │ cdc/ (17 files) │ edge/ (18)   │
│   Vectors, LLM   │  PG/MySQL/Mongo │  Offline WAL │
├──────────────────────────────────────────────────┤
│ cluster/ (20) │ replication/ (13) │ connect/ (17) │
│   Raft consensus│  Geo-replication  │  Kafka Connect│
└──────────────────────────────────────────────────┘
```

### Workspace Structure

The project uses a Cargo workspace with multiple crates:

| Crate | Location | Purpose |
|-------|----------|---------|
| `streamline` (main) | `.` (root) | Server, CLI, and core library |
| `streamline-analytics` | `crates/streamline-analytics` | DuckDB-based SQL analytics engine (isolated due to heavy C deps) |
| `streamline-wasm` | `crates/streamline-wasm` | WASM transform runtime (isolated due to wasmtime dependency) |
| `streamline-operator` | `streamline-operator/` | Kubernetes operator (separate deployment artifact) |

Heavy dependencies like DuckDB and Wasmtime are isolated into workspace crates to keep the main crate's compile time fast when those features are disabled.

---

## How to Contribute

### Good First Issues

Look for issues labeled [`good-first-issue`](https://github.com/streamlinelabs/streamline/labels/good-first-issue). These are carefully scoped tasks ideal for newcomers:

- **Documentation** — fix typos, improve examples, add missing guides
- **Tests** — increase coverage for existing modules
- **CLI** — add or improve command-line flags and output formatting
- **SDK examples** — write sample apps for any of the 7 SDK repos
- **Bug fixes** — issues with clear reproduction steps

### Types of Contributions

| Type | How |
|------|-----|
| **Bug Reports** | File an issue with reproduction steps, expected vs. actual behavior, and version info |
| **Feature Requests** | Open a GitHub Discussion first to gauge interest and alignment |
| **Code** | Fork → branch → PR (see [Development Workflow](#development-workflow) below) |
| **Documentation** | Improvements to docs, tutorials, blog posts, or inline doc comments |
| **SDKs** | Contributions to any of the language SDK repos (Java, Python, Go, Node, Rust, .NET) |
| **WASM Transforms** | Build transforms for the WASM transform marketplace |

### Development Workflow

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/streamline.git
   cd streamline
   ```
3. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```
4. **Make changes** — write code and tests
5. **Validate** before committing:
   ```bash
   cargo fmt && \
   cargo clippy --all-targets --all-features -- -D warnings && \
   cargo test
   ```
6. **Commit** with a conventional message (see [Commit Messages](#commit-messages))
7. **Push** and open a Pull Request against `main`

---

## Code Style

### Formatting

- Run `cargo fmt` before every commit
- Run `cargo clippy --all-targets --all-features -- -D warnings`
- All code must pass CI checks before merge

### Rust Conventions

- **Naming**: `snake_case` for functions/variables, `PascalCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants
- **Visibility**: Default to private. Use `pub(crate)` for internal APIs, `pub` only for genuinely public interfaces.
- **Async**: All async code runs on the Tokio runtime. Never block the async runtime with synchronous I/O or `std::thread::sleep` — use `tokio::time::sleep` instead.
- **Imports**: Group into std → external crates → internal crates, separated by blank lines.

### Error Handling

- Use `StreamlineError` from `src/error.rs` for all error types
- Propagate errors with the `?` operator
- **Never** use `.unwrap()` or `.expect()` in production code paths

```rust
// ✅ Good — descriptive error, uses ? propagation
let value = optional.ok_or(StreamlineError::Storage("segment not found".into()))?;

// ❌ Bad — panics on None
let value = optional.unwrap();
```

### Module-Specific Guidelines

| Module | Guidelines |
|--------|-----------|
| **server/** | HTTP handlers return `Result<Response>`. Add OpenAPI annotations for new routes. |
| **storage/** | All I/O must be async. Segment operations need corresponding cleanup in `Drop`. |
| **protocol/** | Follow Kafka protocol spec exactly. Add wire-format tests for new API keys. |
| **streamql/** | SQL functions must be registered in the catalog. Add parser + planner + execution tests. |
| **ai/** | Vector operations use `f32` arrays. Embedding dimensions must be validated at ingest. |
| **cdc/** | Source connectors implement the `CdcSource` trait. Test with containerized databases. |
| **cluster/** | Raft state changes require both unit tests and multi-node integration tests. |
| **edge/** | Offline WAL must be size-bounded. Test sync-on-reconnect scenarios. |

---

## API Stability

### Stability Levels

| Level | Description | Breaking Changes Allowed |
|-------|-------------|--------------------------|
| **Stable** | Production-ready | Only in major versions |
| **Beta** | Feature-complete, API may shift | In minor versions with CHANGELOG notice |
| **Experimental** | Work in progress | Any time without notice |

### Module Stability Map

| Stability | Modules |
|-----------|---------|
| Stable | `server`, `storage`, `consumer`, `protocol`, `config`, `error`, `metrics` |
| Beta | `embedded`, `transaction`, `cluster`, `replication`, `schema`, `auth`, `audit`, `telemetry` |
| Experimental | `analytics`, `sink`, `streamql`, `cdc`, `edge`, `ai`, `lakehouse`, `timeseries`, `stateful`, `multitenancy`, `autotuning`, `crdt`, `lifecycle`, `obs_pipeline`, `dsl`, `integration`, `network`, `transport`, `observability`, `runtime` |

### Guidelines by Stability

**Stable modules:**
- Avoid breaking changes to public APIs
- Add deprecation warnings before removal
- Maintain backwards compatibility across minor versions

**Beta modules:**
- Can modify APIs but document all changes in CHANGELOG
- Aim for stability but it's not guaranteed

**Experimental modules:**
- Free to change significantly between any versions
- Add stability annotations (`/// # Stability: Experimental`) to new public items

### Deprecation Process

1. Add the `#[deprecated]` attribute with migration guidance:
   ```rust
   #[deprecated(since = "0.5.0", note = "Use `new_function` instead")]
   pub fn old_function() { }
   ```
2. Add an entry to `docs/DEPRECATIONS.md`
3. Keep the deprecated item functional for **at least 2 minor versions**
4. Remove in the subsequent version after the grace period

---

## Testing

### Running Tests

```bash
# Fast feedback (lite edition)
make quick-check               # fmt + clippy
make test-lite                 # tests with default features

# Full test suite
cargo test                     # default features
cargo test --all-features      # everything enabled

# Targeted testing
cargo test storage::           # run only storage module tests
cargo test -- --nocapture      # show stdout/stderr output

# Full validation (what CI runs)
make validate                  # fmt + clippy + tests + doc build
```

### Test Matrix

| Goal | Command | When to Use |
|------|---------|-------------|
| Fast local feedback | `make quick-check && make test-lite` | Default for most edits |
| Full feature coverage | `cargo test --all-features` | Before opening a PR |
| Nextest (if installed) | `cargo nextest run` | Faster parallel execution |
| Compatibility suite | `cargo test --features compatibility-tests protocol_compatibility -- --ignored --nocapture` | Changing Kafka protocol code |

### Writing Tests

- Place unit tests in `#[cfg(test)] mod tests { }` blocks within the same file
- Use `#[tokio::test]` for async tests
- Use the `tempfile` crate for temporary directories (never hard-code `/tmp` paths)
- Name tests descriptively: `test_<what>_<condition>_<expected>`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_produce_message_returns_monotonic_offset() {
        let storage = TestStorage::new().await;
        let offset1 = storage.produce("topic-a", b"hello").await.unwrap();
        let offset2 = storage.produce("topic-a", b"world").await.unwrap();
        assert!(offset2 > offset1, "offsets must be monotonically increasing");
    }

    #[test]
    fn test_config_parse_rejects_negative_retention() {
        let result = Config::parse("retention_ms: -1");
        assert!(result.is_err());
    }
}
```

---

## Pull Requests

### Before Submitting

1. **Run the full validation suite:**
   ```bash
   cargo fmt && \
   cargo clippy --all-targets -- -D warnings && \
   cargo clippy --all-targets --all-features -- -D warnings && \
   cargo test && \
   cargo test --all-features
   ```
2. **Update documentation** if your change affects user-facing behavior
3. **Add or update tests** for new functionality
4. **Update CHANGELOG.md** for user-visible changes

### PR Guidelines

- Keep PRs focused on a **single logical change**
- Write a clear description: what changed, why, and how to test
- Reference related issues (e.g., `Closes #123`)
- Expect at least one review before merge
- CI must pass (fmt, clippy, tests) — reviewers won't look at failing PRs

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat(storage): add segment compaction

Add background compaction for log segments to reclaim space
from deleted records. Runs automatically when segments exceed
the configured threshold.

Closes #123
```

**Prefixes:**

| Prefix | Use For |
|--------|---------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Code restructuring (no behavior change) |
| `test` | Adding or improving tests |
| `perf` | Performance improvement |
| `chore` | Build, CI, or tooling changes |

---

## Code Review Process

- **All PRs require at least one approval** from a maintainer
- **CI must be green** — formatting, linting, and all tests passing
- **Breaking changes** to stable or beta modules require an RFC discussion in GitHub Discussions before implementation
- Reviewers focus on correctness, performance, and API design — not style (that's what `cargo fmt` is for)
- If review comments require significant changes, push new commits (don't force-push) so reviewers can see the diff

---

## Documentation

- Update relevant `.md` files for any user-facing changes
- Add `///` doc comments to all public APIs with at least one usage example
- The Docusaurus docs site (`streamline-docs/`) mirrors some content from `docs/` — keep both in sync
- Keep `CLAUDE.md` updated when architecture changes (it provides context for AI assistants)

---

## Related Repositories

| Repository | What to Contribute |
|------------|-------------------|
| [streamline-java-sdk](https://github.com/streamlinelabs/streamline-java-sdk) | Java/Spring Boot client improvements |
| [streamline-python-sdk](https://github.com/streamlinelabs/streamline-python-sdk) | Async Python client improvements |
| [streamline-go-sdk](https://github.com/streamlinelabs/streamline-go-sdk) | Go client improvements |
| [streamline-node-sdk](https://github.com/streamlinelabs/streamline-node-sdk) | TypeScript/Node.js client improvements |
| [streamline-rust-sdk](https://github.com/streamlinelabs/streamline-rust-sdk) | Rust client improvements |
| [streamline-dotnet-sdk](https://github.com/streamlinelabs/streamline-dotnet-sdk) | .NET client improvements |
| [streamline-operator](https://github.com/streamlinelabs/streamline-operator) | Kubernetes operator and CRDs |
| [terraform-provider-streamline](https://github.com/streamlinelabs/terraform-provider-streamline) | Terraform provider resources |
| [streamline-docs](https://github.com/streamlinelabs/streamline-docs) | Documentation website (Docusaurus) |
| [streamline-deploy](https://github.com/streamlinelabs/streamline-deploy) | Helm charts, Docker images, K8s manifests |

---

## Community

- **Discord** — Real-time chat, help, and discussion: [discord.gg/streamline](https://discord.gg/streamline)
- **GitHub Discussions** — Design discussions, RFCs, and Q&A
- **Weekly Office Hours** — Open Q&A with maintainers (schedule posted in Discord)

## Recognition

Contributors are recognized in [CONTRIBUTORS.md](CONTRIBUTORS.md) and in the release notes for each version. Significant contributions may be highlighted in blog posts and community updates.

## License

By contributing, you agree that your contributions will be licensed under the [Apache-2.0 license](LICENSE).

---

Thank you for helping make streaming simple! 🚀
