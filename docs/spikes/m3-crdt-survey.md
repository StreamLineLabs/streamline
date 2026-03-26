# M3 Spike — CRDT Registry & Plug Surface

> Deliverable for `m3-p0-crdt-survey`. Audits the existing
> `streamline/src/crdt/` surface and pins the v1 built-in CRDT registry +
> custom-merge plug for M3 P1.

## 1. What's already in the tree

`streamline/src/crdt/` ships primitives (verify file names during P1):

- `g_counter.rs` / `pn_counter.rs` — counters. ✅ usable.
- `g_set.rs` — grow-only set. ✅
- `or_set.rs` — observed-remove set. ✅ (verify tag generator under churn).
- `lww_register.rs` — last-writer-wins. ✅ (clock-skew risk — see §4).
- `mv_register.rs` — multi-value register. ✅
- `rga.rs` — replicated growable array. ⚠️ unfinished — exclude from v1.
- `delta.rs` — delta-state mixin. ⚠️ untested at scale; internal only.

Tests exist per type; concurrent-merge property tests are missing for the
⚠️ items.

## 2. Decision — v1 Built-in Registry

Browser SDK exposes **exactly four** CRDT topic kinds at GA:

| Kind         | Underlying type      | When to choose                                    |
|--------------|----------------------|---------------------------------------------------|
| `lww`        | `LwwRegister<T>`     | Single value per key; latest-wins acceptable.     |
| `gset`       | `GSet<T>`            | Append-only set; no removals ever.                |
| `orset`      | `OrSet<T>`           | Set with adds and removes.                        |
| `mvregister` | `MvRegister<T>`      | Concurrent edits surfaced, not merged.            |

Counters deferred to Phase 2 (need quota story on shared topics).
`rga` deferred to M3 P3 (text CRDT deserves dedicated effort).

## 3. Custom-Merge Plug (everything else)

```yaml
topic: collab-doc
crdt:
  kind: custom
  merge_module: ./merges/yjs_merge.wasm
  merge_function: "merge"
```

WASM ABI:

```rust
extern "C" fn merge(
    state_ptr: *const u8, state_len: usize,
    delta_ptr: *const u8, delta_len: usize,
    out_ptr: *mut u8, out_cap: usize,
) -> i64; // bytes written, or -errno
```

Piggy-backs on existing `wasm-transforms` runtime (ADR-0013); no new
runtime code required.

## 4. Risks & Open Items

1. **Clock skew** — `LwwRegister` ties to physical time. M3 P1 spec: the
   broker rewrites `ts` to broker-side timestamp on admit; browser SDK
   MUST NOT trust client clock for ordering. Acceptance test required.
2. **Tombstone footprint** — `OrSet` keeps a tombstone per removal.
   Garbage-collection-via-quorum needed post-P1.
3. **Schema evolution** — CRDTs are extra-painful to evolve. Pin "freeze
   CRDT shape at topic create" rule in M3 P2 docs.
4. **Property tests** — `or_set` and `lww_register` need `proptest`
   harnesses verifying idempotent / commutative / associative merges. New
   file `streamline/tests/crdt_props.rs` in P1.

## 5. Action items rolling into M3 P1

- New `streamline/src/crdt/registry.rs` exposing `built_in(name)`.
- Land `proptest` suite (item 4).
- Define WASM ABI in `streamline/src/edge/sync/custom_merge.rs`
  (`m3-p1-lww-topic`).
- Document clock-skew rewrite in M3 P1 acceptance tests.

## References

- `streamline/src/crdt/`
- ADR-0013: Wasmtime Transform Runtime
- Shapiro et al. 2011, *A Comprehensive Study of CRDTs* —
  <https://hal.inria.fr/inria-00555588>
