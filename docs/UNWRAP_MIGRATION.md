# Unwrap Migration Guide

## Context

Streamline has `clippy::unwrap_used = "deny"` in workspace lints. Files with
legacy `unwrap()` calls are quarantined with `#![allow(clippy::unwrap_used)]`
and an exact count comment. This guide explains how to fix them.

## Quarantined Files

| File | Unwraps | Priority |
|------|---------|----------|
| `storage/topic.rs` | 97 | 🔴 Core write path |
| `storage/partition.rs` | 92 | 🔴 Core write path |
| `transaction/coordinator.rs` | 74 | 🔴 Exactly-once |
| `storage/segment.rs` | 72 | 🔴 Core read/write |
| `server/schema_api.rs` | 70 | 🟡 API handler |
| `embedded.rs` | 66 | 🟡 Library API |
| `storage/wal.rs` | 65 | 🔴 Data durability |
| `sqlite/engine.rs` | 58 | 🟡 Analytics |
| `multitenancy/namespaces.rs` | 57 | 🟡 Multi-tenant |
| `storage/producer_state.rs` | 55 | 🟡 Idempotent |
| `protocol/response_cache.rs` | 19 | 🟡 Cache |
| `protocol/simple.rs` | 16 | 🟢 Simple protocol |
| `protocol/pipeline.rs` | 9 | 🟢 Pipelining |
| `protocol/internal.rs` | 2 | 🟢 Inter-broker |

**Total quarantined: ~752 unwraps across 14 files**

## How to Fix

### Pattern 1: Lock unwraps → `.expect("lock poisoned")`

```rust
// Before:
let guard = self.topics.read().unwrap();

// After (preferred — lock poisoning is unrecoverable):
let guard = self.topics.read().expect("topic lock poisoned — concurrent panic");
```

Note: `expect()` is also warned but more acceptable for lock poisoning since
a poisoned lock indicates a prior panic and recovery is rarely possible.

### Pattern 2: HashMap get → `ok_or()`

```rust
// Before:
let topic = self.topics.get(name).unwrap();

// After:
let topic = self.topics.get(name)
    .ok_or_else(|| StreamlineError::TopicNotFound(name.to_string()))?;
```

### Pattern 3: Parse/decode → `map_err()`

```rust
// Before:
let value: i64 = bytes.get_i64().unwrap();

// After:
let value = bytes.try_get_i64()
    .map_err(|_| StreamlineError::protocol("truncated i64 in record"))?;
```

### Pattern 4: Channel send → log and continue

```rust
// Before:
tx.send(msg).unwrap();

// After:
if tx.send(msg).is_err() {
    tracing::warn!("channel receiver dropped");
}
```

### Pattern 5: Infallible operations → document why

```rust
// Before:
let s = String::from_utf8(valid_ascii.to_vec()).unwrap();

// After (if truly infallible):
// SAFETY: input is validated ASCII from Kafka protocol spec
let s = String::from_utf8(valid_ascii.to_vec())
    .expect("BUG: ASCII validation should have caught this");
```

## Contribution Workflow

1. Pick a file from the table above
2. Open a PR titled: `fix(storage): migrate unwrap() to error handling in topic.rs`
3. Replace unwraps following the patterns above
4. Update the count in the `#![allow]` comment or remove the allow entirely
5. Ensure `cargo test` passes
6. Ensure `cargo clippy --all-features` passes (with the allow removed)

## Tracking Progress

```bash
# Count remaining quarantined files
grep -rl 'allow(clippy::unwrap_used)' src/ | wc -l

# Count total unwraps in quarantined files
for f in $(grep -rl 'allow(clippy::unwrap_used)' src/); do
  echo "$(grep -c 'unwrap()' $f) $f"
done | sort -rn

# Target: 0 quarantined files before v1.0
```

## Good First Issues

These files have the fewest unwraps and are the easiest to fix:

- `protocol/internal.rs` (2 unwraps) — **great first contribution**
- `protocol/pipeline.rs` (9 unwraps) — request pipelining
- `protocol/simple.rs` (16 unwraps) — text protocol

Label: `good first issue`, `unwrap-migration`
