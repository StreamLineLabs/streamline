# Code Quality Remediation

## Summary

Address the code quality and architecture findings from the review by hardening error handling,
making experimental stubs explicit, and reducing monolithic wiring in core server and protocol
paths.

## Current State

- Topic stats and REST endpoints mask storage errors with `unwrap_or(0)` and return misleading
  metrics (src/server/api.rs, src/storage/topic.rs).
- Kafka header versioning and error response creation rely on large match blocks and swallow
  encoding failures (src/protocol/kafka.rs).
- Experimental modules (CDC adapters, WASM pipeline, StreamQL qualified wildcard) behave as if
  implemented or silently ignore unsupported features.
- SimpleProtocol defaults and runtime creation still use `expect` in production paths
  (src/config/mod.rs, src/main.rs).
- Server and HTTP wiring are monolithic, reducing testability (src/server/mod.rs,
  src/server/http.rs).

## Requirements

- Propagate storage errors in topic stats and REST responses; avoid silent defaults.
- Provide deterministic, table-driven Kafka header/response mapping with explicit encode error
  handling.
- Return clear NotImplemented errors for experimental stubs instead of silent success.
- Remove production `unwrap`/`expect` usage in defaults/runtime setup.
- Extract wiring helpers in server/HTTP to reduce SRP pressure without changing behavior.
- Add tests for new error paths and mappings.

## Implementation Tasks

1. **Storage + REST stats hardening**
   - Files: src/storage/topic.rs, src/server/api.rs, src/server/cluster_api.rs,
     src/server/dashboard_api.rs, src/server/http.rs
   - Replace `unwrap_or` defaults with error propagation.
   - Surface partition/total byte counts where available.
   - Add tests for TopicStats error propagation and byte totals.

2. **Kafka header/error response mapping refactor**
   - Files: src/protocol/kafka.rs (plus helper module if needed)
   - Table-driven request/response header version lookups.
   - `create_error_response` returns Result and logs/propagates encoding failures.
   - Add tests for mapping table and error response encoding errors.

3. **Config/runtime safety fixes**
   - Files: src/config/mod.rs, src/storage/segment.rs, src/main.rs
   - Replace `expect` in SimpleProtocolConfig default with const SocketAddr.
   - Fail fast on invalid SegmentSyncMode strings (no silent default).
   - Replace runtime `expect` with logged error + clean exit.
   - Add tests for SegmentSyncMode parsing errors.

4. **Experimental stub explicit errors**
   - Files: src/streamql/planner.rs, src/wasm/pipeline.rs, src/cdc/native_cdc.rs
   - Qualified wildcard returns explicit error.
   - WASM pipeline stage returns NotImplemented unless runtime is wired.
   - CDC adapters return NotImplemented for start/poll/stop.
   - Add tests for each error path.

5. **Server/HTTP wiring extraction**
   - Files: src/server/mod.rs, src/server/http.rs
   - Extract builder helpers for TopicManager/handler wiring and router setup.
   - Preserve behavior; add focused unit tests if needed.

## Acceptance Criteria

- [ ] REST topic stats return accurate offsets/bytes or explicit errors.
- [ ] Kafka header/response mapping is table-driven with explicit encoding error handling.
- [ ] Experimental stubs fail fast with clear errors.
- [ ] No production `unwrap`/`expect` in updated code paths.
- [ ] Tests added for new error paths and mappings.
- [ ] `cargo fmt`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test`
      pass.

## Labels

`code-quality`, `refactor`, `reliability`, `maintenance`, `large`
