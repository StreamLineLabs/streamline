# Streamline Makefile
# Run `make help` to see available targets

.PHONY: all build release dev install test test-lite bench check quick-check fmt clippy doc validate clean help nextest quickstart docker-quickstart
.PHONY: cli-docs config-docs
.PHONY: install-tools security-audit deny coverage watch run cli version-check
.PHONY: test-cross-sdk test-cross-sdk-full

# Default target
all: validate

# ============================================================================
# Build Targets
# ============================================================================

## Build debug binary
build:
	cargo build

## Build release binary (optimized)
release:
	cargo build --release

## Build and run server with defaults
dev:
	cargo run

## Install Streamline binaries to ~/.cargo/bin
install:
	cargo install --path . --locked --bin streamline --bin streamline-cli

## One-command quickstart (builds, runs, initializes demo data)
quickstart:
	./scripts/quickstart.sh

## One-command Docker quickstart (docker compose + demo data)
docker-quickstart:
	./scripts/quickstart-docker.sh

## Run server with custom settings
run:
	cargo run -- --listen-addr 0.0.0.0:9092 --data-dir ./data

# ============================================================================
# Testing Targets
# ============================================================================

## Run all tests
test:
	cargo test --all-features

## Run tests (lite edition only — faster)
test-lite:
	cargo test

## Run tests with cargo-nextest (if installed)
nextest:
	cargo nextest run

## Run tests with output visible
test-verbose:
	cargo test --all-features -- --nocapture

## Run specific test module (usage: make test-mod MOD=storage)
test-mod:
	cargo test $(MOD)::

## Run benchmarks
bench:
	cargo bench

## Compile benchmarks without running
bench-compile:
	cargo bench --no-run

# ============================================================================
# Cross-SDK Testing Targets
# ============================================================================

## Run cross-SDK protocol compatibility tests (fast, no Docker)
test-cross-sdk:
	cargo test --test cross_sdk_test

## Run full cross-SDK integration tests via Docker Compose
test-cross-sdk-full:
	docker compose -f tests/cross_sdk/docker_compose.yml up --build --abort-on-container-exit

# ============================================================================
# Code Quality Targets
# ============================================================================

## Quick compile check (lite edition, ~30s — fastest iteration)
quick-check:
	cargo check

## Check code compiles without building (all features)
check:
	cargo check --all-targets --all-features

## Format code
fmt:
	cargo fmt

## Check formatting without changes
fmt-check:
	cargo fmt --all -- --check

## Run clippy linter
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

## Run all linters (fmt check + clippy)
lint: fmt-check clippy

## Verify version consistency (Cargo.toml, CHANGELOG.md, DOCS.md)
version-check:
	./scripts/check-version-sync.sh

## Build documentation
doc:
	cargo doc --no-deps --all-features

## Generate CLI docs snapshot
cli-docs:
	./scripts/generate-cli-reference.sh

## Generate configuration docs snapshot
config-docs:
	./scripts/generate-config-reference.sh

## Open documentation in browser
doc-open:
	cargo doc --no-deps --all-features --open

## Full validation (format, lint, test, doc)
validate: fmt-check clippy test doc
	@echo "All checks passed!"

# ============================================================================
# Security & Audit Targets
# ============================================================================

## Run security audit
security-audit:
	cargo audit

## Run cargo-deny checks (licenses, advisories, bans)
deny:
	cargo deny check

## Run all security checks
security: security-audit deny

# ============================================================================
# Coverage & Analysis
# ============================================================================

## Generate code coverage report (requires cargo-llvm-cov)
coverage:
	cargo llvm-cov --all-features --html
	@echo "Coverage report: target/llvm-cov/html/index.html"

## Generate coverage and open in browser
coverage-open:
	cargo llvm-cov --all-features --html --open

# ============================================================================
# CLI Tool
# ============================================================================

## Run CLI tool (usage: make cli ARGS="topics list")
cli:
	cargo run --bin streamline-cli -- $(ARGS)

## List topics
topics:
	cargo run --bin streamline-cli -- topics list

## Create a topic (usage: make create-topic NAME=my-topic PARTITIONS=3)
create-topic:
	cargo run --bin streamline-cli -- topics create $(NAME) --partitions $(or $(PARTITIONS),1)

# ============================================================================
# Development Tools
# ============================================================================

## Install development tools
install-tools:
	cargo install cargo-audit cargo-deny cargo-llvm-cov cargo-watch critcmp

## Watch for changes and run tests
watch:
	cargo watch -x test

## Watch for changes and run clippy
watch-clippy:
	cargo watch -x 'clippy --all-targets --all-features'

# ============================================================================
# Cleanup
# ============================================================================

## Clean build artifacts
clean:
	cargo clean

## Clean data directory
clean-data:
	rm -rf ./data

## Clean everything
clean-all: clean clean-data

# ============================================================================
# Help
# ============================================================================

## Show this help message
help:
	@echo "Streamline Development Commands"
	@echo "================================"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | while read line; do \
		target=$$(echo "$$line" | sed 's/:.*//' ); \
		echo "$$line"; \
	done | column -t -s ':' 2>/dev/null || grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'
	@echo ""
	@echo "Quick Start:"
	@echo "  make dev         - Run development server"
	@echo "  make install     - Install streamline and streamline-cli"
	@echo "  make quickstart  - One-command quickstart script"
	@echo "  make quick-check - Fast compile check (~30s)"
	@echo "  make test-lite   - Run tests (lite edition, fast)"
	@echo "  make test        - Run all tests (all features)"
	@echo "  make validate    - Full validation before commit"
	@echo "  make help       - Show this help"
