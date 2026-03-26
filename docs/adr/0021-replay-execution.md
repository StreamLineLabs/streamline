# ADR-0021: Replay Execution Semantics

## Status

Proposed (M5 P0 deliverable)

## Context

`streamline branch run` re-executes a transform over historical data on a
branch. We need to define:

1. What guarantees the replay engine offers (exactly-once vs at-least-once
   on the branch).
2. How the engine integrates with the existing FaaS runtime
   (`streamline/src/faas/`) and WASM transform runtime (ADR-0013).
3. How replay output is materialized into the branch.

## Decision

- **Engine:** reuse the FaaS WASM runtime; replay is "FaaS over a fixed
  offset range, output piped into a branch's owned segments."
- **Determinism contract:** transforms declared `pure: true` in their
  manifest are guaranteed bit-identical reruns. Side-effecting transforms
  (HTTP / DB) are allowed but the engine refuses to checkpoint them.
- **Exactly-once on the branch:** the replay job acquires a lease on
  `(branch_id, range)` and writes outputs idempotently keyed by
  `(input_offset, transform_version)`.
- **Resumability:** progress is checkpointed every N records to
  `__branches.progress`; on crash, the engine resumes from the last
  checkpoint without producing duplicates because keys are deterministic.
- **Throttling:** replay shares the broker's CPU pool; a per-branch token
  bucket caps inflight work at `max_replay_cores` (default 2).
- **Cancellation:** `streamline branch cancel <id>` removes the lease;
  partial outputs remain on the branch (visible, marked
  `replay_status=partial`).

## Consequences

### Positive
- Single execution model for live streams and time-travel; less code.
- Pure-transform contract gives users a clear path to bit-identical reruns
  (essential for ML eval).
- Idempotent keying means retries are safe by construction.

### Negative
- Side-effecting transforms get reduced guarantees; documenting this clearly
  is a doc-effort burden in M5 P3.
- Token-bucket throttle is approximate; a noisy-neighbor branch can still
  inflate p99s on the broker. Hard quotas tracked for M5 P3.

### Neutral
- Replay is read-only on the parent topic — can never corrupt source data.

## References

- ADR-0013: Wasmtime Transform Runtime
- Existing FaaS: `streamline/src/faas/`
- Internal: `MOONSHOT_PLAN.md` Feature M5
