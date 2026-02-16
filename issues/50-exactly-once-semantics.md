# Exactly-Once Semantics Completion

## Summary

Complete the Exactly-Once Semantics (EOS) implementation by filling critical gaps: WriteTxnMarkers handler, server-side READ_COMMITTED isolation filtering, and full producer state persistence including per-partition sequence tracking.

## Current State

- Transaction coordinator with state machine: ✅ Working (`src/transaction/coordinator.rs`)
- Idempotent producers with sequence dedup: ✅ Working (`src/storage/producer_state.rs`)
- InitProducerId, AddPartitionsToTxn, EndTxn, TxnOffsetCommit: ✅ Working
- WriteTxnMarkers: ❌ Stub at `src/protocol/kafka.rs:3226`
- READ_COMMITTED isolation: ❌ Partial — `partition.rs:772-775` only filters by HWM
- Producer state persistence: ❌ Incomplete — `producer_state.rs:235-281` skips partition states
- Fetch path: ❌ Always calls `read()`, never `read_committed()`

## Requirements

### P0: WriteTxnMarkers
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Write COMMIT markers | P0 | Write control records with COMMIT flag to all partitions in a transaction |
| Write ABORT markers | P0 | Write control records with ABORT flag to all partitions in a transaction |
| Coordinator dispatching | P0 | Transaction coordinator should call WriteTxnMarkers on EndTxn |

### P0: READ_COMMITTED Isolation
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Filter aborted records | P0 | partition.read_committed() must skip records from aborted transactions |
| Track aborted transactions | P0 | Maintain set of aborted producer_id+epoch pairs per partition |
| Wire into fetch | P0 | Fetch handler uses read_committed() when isolation_level=1 |

### P1: Producer State Persistence
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Save partition states | P1 | save_state() persists per-partition producer sequences and epochs |
| Load partition states | P1 | load_state() restores per-partition state on startup |

## Implementation Tasks

1. **WriteTxnMarkers**: Complete handler to write COMMIT/ABORT control records to partitions
   - Files: `src/protocol/kafka.rs`, `src/transaction/coordinator.rs`
   - Acceptance: Markers written to all transaction partitions on EndTxn

2. **READ_COMMITTED filtering**: Enhance partition.read_committed() to filter aborted records
   - Files: `src/storage/partition.rs`
   - Acceptance: Aborted records excluded from read_committed() results

3. **Producer state persistence**: Complete save/load for partition-level states
   - Files: `src/storage/producer_state.rs`
   - Acceptance: Sequence numbers survive restart

4. **Fetch integration**: Wire read_committed() into fetch handler
   - Files: `src/protocol/kafka.rs`
   - Acceptance: Fetch with isolation_level=1 uses read_committed()

## Acceptance Criteria

- [ ] WriteTxnMarkers writes COMMIT/ABORT control records to partitions
- [ ] read_committed() filters out records from aborted transactions
- [ ] Producer state (sequences, epochs) persists across restarts
- [ ] Fetch handler respects isolation_level parameter
- [ ] All existing tests pass
- [ ] New unit tests for each component

## Labels

`enhancement`, `feature`, `eos`, `size:medium`
