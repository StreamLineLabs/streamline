# v1.0 Release Criteria

This document defines the objective criteria that must be met before
Streamline can be tagged as v1.0.0 (first stable release).

## Status: 🔴 Not Ready

Last reviewed: 2026-02-21

---

## 1. Stability Requirements

### 1.1 Core Module Stability (must be 100%)
All modules classified as "Stable" in `docs/API_STABILITY.md` must pass
3 consecutive release cycles without breaking changes.

| Module | Stable Since | Consecutive Clean Releases | Status |
|--------|-------------|---------------------------|--------|
| server | 0.1.0 | 0/3 | 🔴 |
| storage | 0.1.0 | 0/3 | 🔴 |
| consumer | 0.1.0 | 0/3 | 🔴 |
| protocol | 0.1.0 | 0/3 | 🔴 |
| config | 0.1.0 | 0/3 | 🔴 |
| error | 0.1.0 | 0/3 | 🔴 |
| analytics | 0.2.0 | 0/3 | 🔴 |
| embedded | 0.2.0 | 0/3 | 🔴 |

### 1.2 Kafka Protocol Compatibility (must be 100%)
All tests in `compatibility.yml` must pass for:
- [ ] Kafka CLI tools: 3.6.0, 3.7.0, 3.8.0
- [ ] librdkafka (confluent-kafka-python)
- [ ] kafka-python
- [ ] kafkajs
- [ ] Sarama (Go)
- [ ] franz-go

### 1.3 SDK Conformance (must be 100% for required tests)
All 46 required tests in `tests/cross_sdk/CONFORMANCE_SPEC.md` must pass
for all 6 SDKs (Java, Python, Go, Node.js, Rust, .NET).

- [ ] Java SDK: 0/46
- [ ] Python SDK: 0/46
- [ ] Go SDK: 0/46
- [ ] Node.js SDK: 0/46
- [ ] Rust SDK: 0/46
- [ ] .NET SDK: 0/46

---

## 2. Quality Requirements

### 2.1 Test Coverage
- [ ] Integration tests: 58+ passing (current: 58)
- [ ] Benchmark suite: All 11 benchmarks run without regression
- [ ] Chaos tests pass (cluster_test.rs, chaos_test.rs)
- [ ] Crash recovery test passes (crash_recovery_test.rs)

### 2.2 Documentation
- [ ] API_STABILITY.md complete and accurate
- [ ] All Stable module APIs have doc comments
- [ ] Getting started guide verified end-to-end
- [ ] Migration guide verified with Kafka 3.7

### 2.3 Security
- [ ] cargo-audit reports 0 known vulnerabilities
- [ ] SECURITY.md updated with v1.0 support policy
- [ ] CVE response process tested with simulated vulnerability
- [ ] No unsafe code without documented justification

---

## 3. Operational Requirements

### 3.1 Performance
- [ ] Benchmark dashboard published to GitHub Pages
- [ ] No performance regression >10% from v0.2.0 baseline
- [ ] Startup time: <100ms (in-memory mode)
- [ ] Memory: <50MB idle

### 3.2 Deployment
- [ ] Docker multi-arch images (amd64, arm64) published
- [ ] Homebrew formula updated and tested
- [ ] Install script (`install.sh`) works on Linux, macOS, WSL
- [ ] Helm chart v1.0 with validated values.yaml
- [ ] Kubernetes operator handles rolling upgrades

---

## 4. Community Requirements

### 4.1 External Validation
- [ ] 10+ external contributors (currently: 0)
- [ ] 5+ documented production or development deployments
- [ ] Published to all package registries (crates.io, npm, PyPI, Maven, NuGet)
- [ ] Testcontainers module submitted to official catalog

### 4.2 Support Infrastructure
- [ ] Discord server operational with >100 members
- [ ] GitHub Discussions enabled
- [ ] Monthly community call established
- [ ] CONTRIBUTING.md verified by external contributor

---

## 5. Release Process

When all criteria above are met:

1. Create `release/1.0` branch from `main`
2. Final review of CHANGELOG.md
3. Bump version to `1.0.0` in Cargo.toml
4. Tag `v1.0.0`
5. Publish to all registries via `publish-sdks.yml`
6. Create GitHub Release with release notes
7. Announce on blog, Discord, Hacker News, Twitter/X
8. Create `lts/1.0` branch for long-term support

### LTS Policy

| Branch | Support Duration | Backports |
|--------|-----------------|-----------|
| `lts/1.0` | 12 months from release | Critical security fixes, data loss bugs |
| `main` | Rolling | All changes |
| `release/X.Y` | Until next minor | Bug fixes |

### Post-v1.0 Versioning

- **PATCH** (1.0.x): Bug fixes, security patches
- **MINOR** (1.x.0): New features, backward-compatible
- **MAJOR** (x.0.0): Breaking changes (with 2-version deprecation)
