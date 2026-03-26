# ADR-0022: Browser Build Feature Selection

## Status

Proposed (M3 P0 deliverable)

## Context

M3 (Streamline Anywhere) ships a `wasm32-unknown-unknown` build of the core
that runs in a browser tab. Hard size budget: **≤ 800 KB gzipped** for the
default browser bundle. The full server build is many MB and includes
features that are nonsensical (or impossible) in a browser: Raft
consensus, Iceberg writers, native TLS, mmap segment storage,
`tokio::net`, etc.

## Decision

A new `wasm-browser` workspace member at
`streamline-browser/` selects only the strictly-required features:

**Included:**
- Record framing / batching (`streamline/src/protocol/kafka/records.rs`).
- HNSW-lite vector index (M2-compatible) for client-side semantic ops.
- CRDT merges (`streamline/src/crdt/`) for offline-first.
- IndexedDB-backed segment storage (browser-only impl).
- WebSocket transport (default) + WebTransport (feature-gated, see
  ADR-0023).

**Excluded** (gated by `not(target_arch="wasm32")`):
- All clustering / Raft.
- Iceberg, Parquet, DuckDB.
- All KMS adapters except `LocalAge` (which falls back to WebCrypto via a
  thin shim).
- TLS — handled by the browser's TLS stack.
- File-system storage — replaced by IndexedDB driver.

**Conditional compilation pattern** (proposed):

```rust
#[cfg(target_arch = "wasm32")]
pub use wasm_storage::IndexedDbSegment as Segment;

#[cfg(not(target_arch = "wasm32"))]
pub use disk_storage::FileSegment as Segment;
```

Build: `cargo build --target wasm32-unknown-unknown --no-default-features
--features browser-min`. CI gate fails the build if gzipped binary > 800
KB.

## Consequences

### Positive
- Single source tree; no fork. New broker features automatically considered
  for browser eligibility via cfg gates.
- Hard CI size gate prevents regressions.
- IndexedDB swap leaves the segment-API stable for higher layers.

### Negative
- Conditional compilation discipline is now load-bearing; PR template
  should remind reviewers.
- `LocalAge` over WebCrypto means weaker key custody than server-side; OK
  for browser personas (per-user, per-device).

### Neutral
- Tooling: `wasm-bindgen` + `wasm-opt -Oz` are added to the dev
  prerequisites.

## References

- `wasm-bindgen`: <https://rustwasm.github.io/wasm-bindgen/>
- IndexedDB API: <https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API>
- Internal: `MOONSHOT_PLAN.md` Feature M3
