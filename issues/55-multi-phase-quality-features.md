# Multi-Phase Quality & Feature Implementation

## Summary

Address critical code quality issues and implement strategic features to move Streamline toward production readiness. This encompasses 7 phases: `.unwrap()` cleanup (critical), Schema Registry GA, Kafka Connect compatibility, production-grade clustering, WASM stream processing, AI semantic routing, and EmbeddedStreamline polish.

## Current State

- **Code Quality**: 1,509 `.unwrap()` + 254 `.expect()` calls in `src/` violate project policy. Panics in async Tokio tasks can crash the runtime.
- **Schema Registry**: Beta — Confluent-compatible API, 3 formats, but in-memory only (no persistence to `_schemas` topic)
- **Kafka Connect**: REST API complete (20+ endpoints), but runtime task execution is stubbed
- **Clustering**: 70% production-ready — Raft consensus works, but JSON file persistence and no metrics
- **WASM**: Scaffolded with Wasmtime v27, pipeline orchestration and module deployment stubbed
- **AI Routing**: Core engine works with hash/TF-IDF embeddings, no real provider integration
- **Embedded**: Beta with 30+ tests, missing consumer groups, compression, schema validation

## Requirements

### Phase 1: .unwrap()/.expect() Cleanup (Critical)
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Clippy enforcement | P0 | Add `deny(clippy::unwrap_used)` to lib.rs |
| Storage fixes | P0 | Fix 313 unwraps in storage layer (partition, segment, compaction, producer_state) |
| Consumer fixes | P0 | Fix 47 unwraps in consumer/coordinator.rs |
| Server fixes | P1 | Fix 90 unwraps/expects in runtime, schema_api, limits |
| Experimental fixes | P1 | Fix ~250+ unwraps/expects across 10+ experimental modules |
| CI validation | P0 | All tests pass, clippy clean |

### Phase 2: Schema Registry GA
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Persistence | P0 | Write/replay events to `_schemas` internal topic |
| Schema references | P1 | Validate and resolve transitive schema references |
| Protobuf validation | P1 | Descriptor compilation for binary validation |
| Hard-delete | P1 | Subject permanent removal |
| Integration tests | P0 | Persistence/recovery, Confluent compatibility |

### Phase 3: Kafka Connect Compatibility
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Task execution | P0 | ConnectorRuntime actually executes source/sink tasks |
| Offset persistence | P0 | Persist to connect-offsets, recover on restart |
| Config persistence | P0 | Persist to connect-configs, recover on restart |
| Status/DLQ | P1 | Real status reporting, dead letter queue |
| Native SDK | P1 | Complete SDK, reference connectors |
| Retry policies | P1 | Exponential backoff, error tolerance |

### Phase 4: Production-Grade Clustering
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Binary WAL | P0 | Replace JSON persistence with binary log storage |
| Health metrics | P0 | Prometheus metrics for cluster health |
| RPC improvements | P1 | Connection pooling, retry logic, timeouts |
| Auto-membership | P1 | Automated rebalancing on join/leave |
| Partition handling | P1 | Split-brain conflict resolution, fencing tokens |
| Snapshot optimization | P2 | Adaptive policy, incremental snapshots |

### Phase 5: WASM Stream Processing
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Pipeline orchestration | P0 | Module deploy/start/stop/undeploy lifecycle |
| Hot reload | P1 | Module update without downtime |
| ABI definition | P0 | Stable ABI, guest SDK examples |
| Security audit | P0 | Resource limits, host function exposure |
| Benchmarks | P1 | WASM vs native throughput comparison |

### Phase 6: AI Semantic Routing
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Real providers | P0 | OpenAI, Cohere, Hugging Face integration |
| Persistent vectors | P1 | Disk-backed vector store with HNSW index |
| Cost control | P1 | Caching, rate limiting, budgeting |

### Phase 7: EmbeddedStreamline Polish
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Consumer groups | P1 | Group coordination for embedded consumers |
| Schema validation | P1 | Wire schema registry for produce/consume |
| Compression | P2 | LZ4/Zstd/Snappy for embedded records |
| Documentation | P1 | API docs, example projects |

## Implementation Tasks

### Phase 1 (6 tasks)
1. Add clippy lint enforcement — `src/lib.rs`, CI config
2. Fix storage layer unwraps — `src/storage/partition.rs`, `segment.rs`, `compaction.rs`, `producer_state.rs`, `timetravel.rs`
3. Fix consumer/coordinator unwraps — `src/consumer/coordinator.rs`
4. Fix protocol/server unwraps — `src/runtime/mod.rs`, `src/server/schema_api.rs`, `src/server/limits.rs`
5. Fix experimental module unwraps — 10+ modules across streamql, connect, marketplace, etc.
6. Validate: `cargo test --all-features` + `cargo clippy --all-targets --all-features`

### Phase 2 (5 tasks)
1. Implement `_schemas` topic persistence + replay
2. Schema references validation
3. Protobuf descriptor compilation
4. Hard-delete and transitive delete
5. Integration tests + stability promotion

### Phase 3 (7 tasks)
1. Connector task execution (source poll, sink put)
2. Offset persistence to connect-offsets
3. Config persistence to connect-configs
4. Status reporting + DLQ
5. Native connector SDK + reference connectors
6. Retry policies
7. Integration tests

### Phase 4 (7 tasks)
1. Binary WAL for Raft storage
2. Cluster health Prometheus metrics
3. Connection pooling + RPC retry
4. Automated membership rebalancing
5. Network partition conflict resolution
6. Snapshot optimization
7. Integration tests

### Phase 5 (6 tasks)
1. Pipeline orchestration (module lifecycle)
2. Hot reload support
3. ABI definition + guest SDK
4. Security audit + sandboxing
5. Performance benchmarks
6. Integration tests

### Phase 6 (4 tasks)
1. Real embedding provider integration
2. Persistent vector storage + HNSW
3. Caching + cost control
4. Integration tests

### Phase 7 (4 tasks)
1. Consumer group support
2. Schema validation integration
3. Compression support
4. Documentation + examples

## Acceptance Criteria

- [ ] Zero `.unwrap()`/`.expect()` calls in production code (test code exempt)
- [ ] `cargo clippy --all-targets --all-features -- -D clippy::unwrap_used` passes
- [ ] Schema Registry persists to `_schemas` topic and recovers on restart
- [ ] Kafka Connect connectors actually execute and persist offsets
- [ ] Clustering uses binary WAL, reports Prometheus metrics
- [ ] WASM modules can be deployed, executed, and hot-reloaded
- [ ] AI routing works with real embedding providers
- [ ] EmbeddedStreamline supports consumer groups and schema validation
- [ ] All existing tests continue to pass
- [ ] Documentation updated for all changed modules

## Related Files

- `src/lib.rs` — Crate-level lints, module re-exports
- `src/storage/partition.rs`, `segment.rs`, `compaction.rs` — Storage unwraps
- `src/consumer/coordinator.rs` — Consumer group unwraps
- `src/schema/` — Schema Registry (7 files)
- `src/connect/` — Kafka Connect (6 files)
- `src/cluster/` — Clustering (14 files)
- `crates/streamline-wasm/` — WASM runtime
- `src/ai/` — AI routing
- `src/embedded.rs` — Embedded SDK

## Labels

`enhancement`, `quality`, `feature`, `extra-large`
