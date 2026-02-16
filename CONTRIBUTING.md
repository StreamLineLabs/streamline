# Contributing to Streamline

Thank you for your interest in contributing to Streamline! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Rust 1.75 or later
- Git
- `pkg-config` (Linux: `apt-get install pkg-config`, macOS: `brew install pkg-config`)
- Node.js 18+ (only required for the website docs)
- Docker (optional, for container workflows)
- `pre-commit` (optional, for git hooks)
- `cargo-nextest` (optional, for faster test runs)
- `sccache` (recommended, for faster rebuilds: `cargo install sccache`)

### Development Setup

```bash
# Clone the repository
git clone https://github.com/josedab/streamline.git
cd streamline

# Optional: bootstrap a full dev environment
./scripts/dev-setup.sh --full

# Build the project
cargo build

# Install binaries for local CLI usage
make install

# Fast checks
make quick-check
make test-lite

# Run with debug logging
RUST_LOG=debug cargo run
```

For more workflows (devcontainer, scripts, and tooling), see
[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md).

### Build Editions

Streamline has two build editions:

| Edition | Command | Features |
|---------|---------|----------|
| **Lite** (default) | `cargo build` | Core streaming, TLS, compression |
| **Full** | `cargo build --features full` | + Auth, clustering, telemetry, cloud storage |

### Cargo Aliases

The project provides shortcuts in `.cargo/config.toml` for common operations:

| Alias | Expands To | Purpose |
|-------|-----------|---------|
| `cargo c` | `cargo check --all-features` | Quick compilation check |
| `cargo dev` | `cargo watch -x 'check --all-features'` | Watch mode (requires `cargo-watch`) |
| `cargo lint` | `cargo clippy --all-targets --all-features -- -D warnings` | Lint with strict warnings |
| `cargo t` | `cargo test --all-features` | Run all tests |
| `cargo lite` | `cargo build` | Build lite edition |
| `cargo full` | `cargo build --features full` | Build full edition |
| `cargo rel` | `cargo build --release` | Release build |
| `cargo docs` | `cargo doc --no-deps --all-features --open` | Build and open docs |
| `cargo play` | `cargo run --bin streamline -- --playground` | Server in playground mode |
| `cargo top` | `cargo run --bin streamline-cli -- top` | TUI dashboard |
| `cargo unused` | `cargo machete` | Check unused deps (requires `cargo-machete`) |

### Build Caching with sccache

For faster rebuilds across branch switches and clean builds, install [sccache](https://github.com/mozilla/sccache):

```bash
cargo install sccache
```

Then uncomment the `rustc-wrapper` line in `.cargo/config.toml`:

```toml
rustc-wrapper = "sccache"
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

## Code Style

### Formatting

- Run `cargo fmt` before committing
- Run `cargo clippy --all-targets --all-features -- -D warnings`
- All code must pass CI checks

### Rust Conventions

- Use `snake_case` for functions and variables
- Use `PascalCase` for types and traits
- Use `SCREAMING_SNAKE_CASE` for constants
- Default to private visibility; use `pub(crate)` for internal APIs

### Error Handling

- Use `StreamlineError` from `src/error.rs`
- Propagate errors with `?` operator
- **Never** use `.unwrap()` or `.expect()` in production code paths

```rust
// Good
let value = optional.ok_or(StreamlineError::Storage("not found".into()))?;

// Bad
let value = optional.unwrap();
```

## API Stability

When contributing, understand the stability levels:

### Stability Levels

| Level | Description | Breaking Changes |
|-------|-------------|------------------|
| **Stable** | Production-ready | Only in major versions |
| **Beta** | Feature-complete | In minor versions with CHANGELOG notice |
| **Experimental** | Work in progress | Any time without notice |

### Module Stability

| Stability | Modules |
|-----------|---------|
| Stable | `server`, `storage`, `consumer`, `protocol`, `config`, `error`, `metrics` |
| Beta | `embedded`, `transaction`, `cluster`, `replication`, `schema`, `auth`, `audit`, `telemetry` |
| Experimental | `analytics`, `sink`, `streamql`, `cdc`, `edge`, `ai`, `lakehouse`, `timeseries`, `stateful`, `multitenancy`, `autotuning`, `crdt`, `lifecycle`, `obs_pipeline`, `dsl`, `integration`, `network`, `transport`, `observability`, `runtime` |

### Guidelines by Stability

**Stable modules:**
- Avoid breaking changes to public APIs
- Add deprecation warnings before removal
- Maintain backwards compatibility

**Beta modules:**
- Can modify APIs but document changes in CHANGELOG
- Aim for stability but not guaranteed

**Experimental modules:**
- Free to change significantly
- Add stability documentation to new public items

### Deprecation Process

When deprecating a feature:

1. Add `#[deprecated]` attribute:
   ```rust
   #[deprecated(since = "0.5.0", note = "Use `new_function` instead")]
   pub fn old_function() { }
   ```

2. Add entry to `docs/DEPRECATIONS.md`

3. Keep functional for 2 minor versions minimum

4. Remove in subsequent version

## Testing

### Running Tests

```bash
# Fast feedback (lite edition)
make quick-check
make test-lite

# Run all tests (default features)
cargo test

# Run tests with output visible
cargo test -- --nocapture

# Run specific test module
cargo test storage::

# Run with all features
cargo test --all-features

# Full validation (fmt, clippy, tests, docs)
make validate
```

### Test Levels

- **Fast:** `make quick-check` and `make test-lite`
- **Full:** `cargo test --all-features`
- **Feature suites:** enable specific flags (e.g., `--features integration-tests`)

### Test Matrix (What to Run)

| Goal | Command | Notes |
|------|---------|-------|
| Fast local feedback | `make quick-check && make test-lite` | Default for most edits |
| Full feature coverage | `cargo test --all-features` | Longer run |
| Nextest (if installed) | `cargo nextest run` | Faster, clearer output |
| Compatibility suite | `cargo test --features compatibility-tests protocol_compatibility -- --ignored --nocapture` | Requires external Kafka tools |

### Writing Tests

- Place unit tests in `#[cfg(test)] mod tests { }` blocks
- Use `#[tokio::test]` for async tests
- Use `tempfile` crate for temporary directories
- Name tests descriptively: `test_<what>_<condition>_<expected>`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_produce_message_returns_offset() {
        // Test implementation
    }
}
```

## Pull Requests

### Before Submitting

1. Run the full validation suite:
   ```bash
   cargo fmt && \
   cargo clippy --all-targets -- -D warnings && \
   cargo clippy --all-targets --all-features -- -D warnings && \
   cargo test && \
   cargo test --all-features
   ```

2. Update documentation if needed

3. Add tests for new functionality

### PR Guidelines

- Keep PRs focused on a single change
- Write clear, descriptive commit messages
- Reference any related issues
- Update CHANGELOG.md for user-facing changes

### Commit Messages

Use clear, descriptive commit messages:

```
feat(storage): add segment compaction

Add background compaction for log segments to reclaim space
from deleted records. Runs automatically when segments exceed
the configured threshold.

Closes #123
```

Prefixes:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance tasks

## Documentation

- Update relevant `.md` files for user-facing changes
- Add doc comments to public APIs
- Include examples in documentation where helpful
- Keep CLAUDE.md updated for AI assistant context

## Subprojects

- **streamline-operator**: Kubernetes operator (see `streamline-operator/README.md`)
- **terraform-provider-streamline**: Terraform provider (`terraform-provider-streamline/README.md`)
- **SDKs**: language clients in `sdks/` (see each `README.md`)

## Getting Help

- Open an issue for bugs or feature requests
- Check existing issues before creating new ones
- Join discussions in issue comments

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 license.
