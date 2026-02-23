# Public Launch Checklist

Operational checklist for the StreamlineLabs public launch.
Execute sequentially — each section must complete before the next.

## Phase 1: Registry Readiness (Day -7)

### Package Name Verification
- [ ] `streamline` available on crates.io
- [ ] `streamline-client` available on crates.io
- [ ] `@streamlinelabs/streamline` available on npm
- [ ] `@streamlinelabs/streamline-wasm` available on npm
- [ ] `streamline-sdk` available on PyPI
- [ ] `io.streamline:streamline-client` available on Maven Central
- [ ] `Streamline.Client` available on NuGet
- [ ] `github.com/streamlinelabs/streamline-go-sdk` on pkg.go.dev

### Metadata Audit
Run for each SDK:
```bash
# Rust
grep -E 'description|repository|homepage|documentation|keywords|categories|license' Cargo.toml

# Node.js
jq '{name, version, description, repository, homepage, license, keywords}' package.json

# Python
grep -E 'name|version|description|license|repository' pyproject.toml

# Java
grep -E '<groupId>|<artifactId>|<version>|<description>|<url>|<license>' pom.xml | head -12

# .NET
grep -E 'PackageId|Version|Description|RepositoryUrl|License' *.csproj
```

Required fields per registry:
| Field | crates.io | npm | PyPI | Maven | NuGet | go |
|-------|-----------|-----|------|-------|-------|-----|
| name | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| version | ✅ | ✅ | ✅ | ✅ | ✅ | tag |
| description | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| license | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| repository | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| homepage | opt | opt | opt | opt | opt | — |
| keywords | opt | opt | opt | — | opt | — |

### CI Pre-flight
- [ ] Run `publish-sdks.yml` with `dry_run: true` — all 7 jobs succeed
- [ ] Run `compatibility.yml` — all Kafka versions pass
- [ ] Run `sdk-conformance.yml` — all SDKs pass
- [ ] Run `cargo test` on core server — all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D warnings` — clean

---

## Phase 2: Registry Publication (Day 0 - Morning)

Execute `publish-sdks.yml` with `dry_run: false`:
```bash
gh workflow run publish-sdks.yml -f version=0.2.0 -f dry_run=false
```

Verify each registry:
```bash
# Rust
cargo install streamline --version 0.2.0 --dry-run

# Node.js
npm info @streamlinelabs/streamline

# Python
pip install streamline-sdk==0.2.0 --dry-run

# Go
go get github.com/streamlinelabs/streamline-go-sdk@v0.2.0

# .NET
dotnet add package Streamline.Client --version 0.2.0
```

Deploy install script:
```bash
# Upload install.sh to CDN / GitHub Pages
# Verify: curl -fsSL https://get.streamline.dev | sh --help
```

---

## Phase 3: Launch Content (Day 0 - Afternoon)

### Hacker News "Show HN" Post
**Title:** `Show HN: Streamline – The Redis of Streaming (Kafka-compatible, <10MB, zero config)`

**Text:**
```
Hi HN! I built Streamline, a streaming platform that aims to do for event
streaming what Redis did for caching: make it trivially easy.

- Single binary, <10MB, zero dependencies (no JVM, no ZooKeeper)
- Full Kafka protocol compatible (50+ APIs, works with existing clients)
- SDKs for 8 languages (Java, Python, Go, Node, Rust, .NET, WASM, C FFI)
- Multi-protocol: Kafka + MQTT + AMQP + gRPC in one process
- Built-in SQL on streams (StreamQL), WASM transform marketplace
- ARM64 edge appliance with MQTT bridge
- GitOps native: plan/apply/watch for streaming config

Benchmarks: ~45ms startup, ~15MB idle memory vs Kafka's ~12s / ~500MB.

It's Apache-2.0, written in Rust.

Install: curl -fsSL https://get.streamline.dev | sh
Docs: https://streamlinelabs.dev
GitHub: https://github.com/streamlinelabs/streamline
Benchmarks: https://streamlinelabs.dev/benchmarks
Interactive playground: https://streamlinelabs.dev/playground
```

### Reddit Posts
- [ ] r/rust: "Streamline: A Kafka-compatible streaming platform in Rust"
- [ ] r/programming: "Show r/programming: The Redis of Streaming"
- [ ] r/selfhosted: "Streamline: Single-binary Kafka alternative for self-hosting"
- [ ] r/kubernetes: "Streamline K8s Operator: 4 CRDs for streaming"

### Twitter/X Thread
```
🚀 Launching Streamline — "The Redis of Streaming"

Kafka-compatible. Single binary. <10MB. Zero config. 8 SDKs.

Thread 🧵👇

1/ The problem: Kafka is powerful but operationally painful.
   JVM + ZooKeeper/KRaft + broker config + connector config = days of setup.

2/ Streamline: ./streamline
   That's it. Server is running. Kafka clients work unchanged.

3/ Numbers:
   - Binary: 8MB vs 300MB
   - Startup: 45ms vs 12s
   - Memory: 15MB vs 500MB
   - Config: 0 files vs dozens

4/ But it's not just "small Kafka." It also has:
   - MQTT + AMQP + gRPC (IoT gateway in one binary)
   - StreamQL (SQL on streams, no ksqlDB needed)
   - WASM transform marketplace
   - Built-in ML feature store

5/ Try it now:
   curl -fsSL https://get.streamline.dev | sh
   🎮 Playground: streamlinelabs.dev/playground
   📊 Benchmarks: streamlinelabs.dev/benchmarks
```

### Discord
- [ ] Server created and configured
- [ ] Channels: #general, #help, #contributing, #showcase, #announcements
- [ ] Welcome message with quickstart link
- [ ] First 5 champion users invited

---

## Phase 4: Community Activation (Day 0-7)

- [ ] Run `apply-labels.sh` across all 16 repos
- [ ] Run `create-good-first-issues.sh` to seed 27+ issues
- [ ] Enable GitHub Discussions on streamline repo
- [ ] Pin 5 most approachable good-first-issues
- [ ] Post welcome message in Discord #announcements
- [ ] Submit Testcontainers modules to official catalog PRs

---

## Phase 5: Post-Launch Monitoring (Day 1-30)

### Metrics to Track
| Metric | Target (Day 7) | Target (Day 30) |
|--------|-----------------|-------------------|
| GitHub stars | 200 | 1,000 |
| Discord members | 30 | 100 |
| Package downloads (total) | 500 | 2,000 |
| External contributors | 2 | 10 |
| Issues opened by community | 5 | 20 |
| Blog posts / mentions | 2 | 5 |

### Response SLAs
| Channel | Response Time |
|---------|--------------|
| GitHub Issues | <24 hours |
| Discord #help | <4 hours |
| Security reports | <48 hours |
| Pull requests | <48 hours |
