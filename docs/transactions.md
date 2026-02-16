# Transactions

Streamline implements Kafka-compatible transactions for exactly-once semantics (EOS).  
Transactions guarantee atomic writes across multiple topic-partitions: either every message in a transaction is committed and visible to consumers, or none are.

## Architecture Overview

The transaction subsystem consists of three main components:

| Component | Source | Purpose |
|---|---|---|
| **TransactionCoordinator** | `src/transaction/coordinator.rs` | Manages transaction lifecycle, timeout, persistence |
| **Transaction / TransactionState** | `src/transaction/state.rs` | Per-transaction state machine and partition tracking |
| **TransactionLog** | `src/transaction/log.rs` | Append-only log + snapshots for crash recovery |
| **EosManager** | `src/transaction/eos.rs` | Idempotent producer sequence tracking (dedup) |

Protocol handlers are in:
- `src/protocol/handlers/transactions.rs` — dispatch layer
- `src/protocol/kafka/transaction_handlers.rs` — Kafka wire-protocol request/response handling

## Exactly-Once Semantics

Exactly-once delivery in Streamline relies on two layers:

1. **Idempotent Producer** — Sequence numbers per (producer_id, topic, partition) detect and suppress duplicate writes.  
2. **Transactions** — Atomic commits across partitions ensure all-or-nothing visibility.

Combined with `read_committed` isolation on the consumer side, this provides end-to-end exactly-once processing.

## Transaction Lifecycle

```
InitProducerId → Ongoing → (produce records)
                       ↓
              AddPartitionsToTxn (register partitions)
              AddOffsetsToTxn   (register consumer group)
              TxnOffsetCommit   (commit offsets in txn)
                       ↓
                   EndTxn(commit=true)    EndTxn(commit=false)
                       ↓                       ↓
                 PrepareCommit           PrepareAbort
                       ↓                       ↓
                 CompleteCommit          CompleteAbort
                       ↓                       ↓
                   (removed)               (removed)
```

### State Machine

| State | Can Add Partitions? | Can End? | Terminal? |
|---|---|---|---|
| `Ongoing` | ✅ | ✅ | ❌ |
| `PrepareCommit` | ❌ | ❌ | ❌ |
| `PrepareAbort` | ❌ | ❌ | ❌ |
| `CompleteCommit` | ❌ | ❌ | ✅ |
| `CompleteAbort` | ❌ | ❌ | ✅ |
| `Dead` | ❌ | ❌ | ✅ |

## Kafka Protocol APIs

| API Key | Name | Version Range | Description |
|---|---|---|---|
| 22 | `InitProducerId` | 0–4 | Allocate producer ID + epoch |
| 24 | `AddPartitionsToTxn` | 0–4 | Register topic-partitions in current txn |
| 25 | `AddOffsetsToTxn` | 0–3 | Register consumer group for offset commits |
| 26 | `EndTxn` | 0–3 | Commit or abort the transaction |
| 28 | `TxnOffsetCommit` | 0–3 | Commit consumer offsets within the txn |
| 27 | `WriteTxnMarkers` | 0–1 | Write COMMIT/ABORT control records |
| 61 | `DescribeProducers` | 0 | Introspect active producers on a partition |
| 65 | `DescribeTransactions` | 0 | Describe state of a transaction |
| 66 | `ListTransactions` | 0 | List all tracked transactions |

## Configuration Reference

### TransactionTimeoutConfig

| Field | Default | Description |
|---|---|---|
| `check_interval_ms` | 10 000 | How often the timeout checker runs (ms) |
| `write_abort_markers` | `true` | Write ABORT control records for timed-out txns |
| `fence_on_timeout` | `true` | Bump producer epoch on timeout (prevent zombies) |
| `min_timeout_ms` | 1 000 | Minimum allowed client timeout |
| `max_timeout_ms` | 900 000 | Maximum allowed client timeout (15 min) |

Client-requested timeouts are clamped to `[min_timeout_ms, max_timeout_ms]`.

### Persistence

In persistent mode, the coordinator saves state to `<data_dir>/transactions/transactions.json` after every mutation.  
The `TransactionLog` provides an additional append-only log with snapshot support under `<data_dir>/__transaction_log/`.

In-memory mode skips all I/O (used in tests and the `--in-memory` server flag).

## Usage Examples

### Single-Partition Transaction

```rust
// Coordinator setup (simplified)
let coordinator = TransactionCoordinator::in_memory(psm, tm, None)?;

// Begin
let (pid, epoch) = coordinator.begin_transaction("my-txn", Some(60_000))?;

// Register partition
coordinator.add_partitions_to_txn("my-txn", pid, epoch, vec![("orders".into(), 0)])?;

// … produce records via the Kafka produce API …

// Commit
coordinator.end_transaction("my-txn", pid, epoch, true)?;
```

### Multi-Partition / Multi-Topic

```rust
let partitions = vec![
    ("orders".into(), 0),
    ("orders".into(), 1),
    ("audit-log".into(), 0),
];
coordinator.add_partitions_to_txn("my-txn", pid, epoch, partitions)?;
// All-or-nothing commit
coordinator.end_transaction("my-txn", pid, epoch, true)?;
```

### Consume-Transform-Produce

```rust
// 1. Begin transaction
let (pid, epoch) = coordinator.begin_transaction("ctp-txn", None)?;

// 2. Add output partitions
coordinator.add_partitions_to_txn("ctp-txn", pid, epoch,
    vec![("output-topic".into(), 0)])?;

// 3. Register consumer group
coordinator.add_offsets_to_txn("ctp-txn", pid, epoch, "my-group")?;

// 4. Commit consumed offsets within the transaction
coordinator.txn_offset_commit("ctp-txn", pid, epoch, "my-group",
    vec![("input-topic".into(), 0, 150, None)])?;

// 5. Commit everything atomically
coordinator.end_transaction("ctp-txn", pid, epoch, true)?;
```

## Isolation Levels

| Level | Value | Behavior |
|---|---|---|
| `READ_UNCOMMITTED` | 0 | Consumer sees all messages including in-flight txn data |
| `READ_COMMITTED` | 1 | Consumer only sees data up to the **Last Stable Offset** (LSO) |

The LSO is computed from `get_first_unstable_offset()` — the minimum base offset of any ongoing transaction on a partition.  When there are no ongoing transactions, LSO equals the high watermark.

Fetch responses include an `aborted_transactions` list so that `READ_COMMITTED` consumers can filter out records from aborted transactions.

## Performance Considerations

- **DashMap sharding** — The `TransactionCoordinator` uses `DashMap` for the transaction registry, providing fine-grained per-transaction locking with no global lock bottleneck.
- **Concurrent transactions** — Different transactional IDs operate independently; adding partitions to one transaction does not block another.
- **Timeout checker** — Runs as a Tokio background task with configurable interval; uses `MissedTickBehavior::Skip` to avoid thundering-herd on slow ticks.
- **Persistence overhead** — Each state mutation writes `transactions.json` synchronously in persistent mode. For high-throughput workloads, consider `--in-memory` or increasing `check_interval_ms`.
- **Epoch fencing** — Uses atomic epoch bumps in the `ProducerStateManager` to fence zombie producers without locking the coordinator.

## Comparison with Apache Kafka

| Feature | Kafka | Streamline |
|---|---|---|
| Transaction protocol | Full KIP-98 | Full KIP-98 compatible |
| Two-phase commit | ✅ | ✅ (PrepareCommit → CompleteCommit) |
| Idempotent producer | ✅ (KIP-98) | ✅ (EosManager) |
| READ_COMMITTED | ✅ | ✅ (LSO + aborted txn list) |
| Transaction timeout | Configurable | Configurable with clamping |
| Transaction log | __transaction_state topic | JSON file + append log |
| Coordinator election | Via internal topic | Single-node (built-in coordinator) |
| Cross-cluster txns | ❌ | ❌ |
| Max partitions/txn | Unlimited | Configurable |

### Key differences

1. **No `__transaction_state` internal topic** — Streamline uses file-based persistence instead of a compacted topic.
2. **Single coordinator** — In single-node mode, the local server is always the transaction coordinator. No leader election needed.
3. **Simpler recovery** — JSON snapshots + append log instead of compacted topic replay.

## Error Codes

| Code | Name | Meaning |
|---|---|---|
| 0 | `NONE` | Success |
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | Topic or partition does not exist |
| 15 | `COORDINATOR_NOT_AVAILABLE` | Transaction coordinator is disabled or unavailable |
| 47 | `INVALID_PRODUCER_EPOCH` | Producer epoch doesn't match (fenced) |
| 48 | `INVALID_TXN_STATE` | Operation not valid for current transaction state |
| 49 | `INVALID_PRODUCER_ID_MAPPING` | Producer ID doesn't match transaction |
| 51 | `CONCURRENT_TRANSACTIONS` | Another transaction is in progress |
| 52 | `TRANSACTION_COORDINATOR_FENCED` | Coordinator epoch mismatch |
| 53 | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | ACL denied |
| 90 | `PRODUCER_FENCED` | Producer was fenced by a new instance |

## Testing

E2E tests are in `tests/transaction_e2e_test.rs` and cover:

- Bulk produce + commit (all-or-nothing)
- Abort verification
- Transaction timeout → automatic abort
- Concurrent transactions on the same partition
- Multi-partition / multi-topic transactions
- Idempotent producer sequence tracking
- READ_COMMITTED isolation semantics
- Consume-transform-produce pattern

Unit tests are embedded in each module (`state.rs`, `coordinator.rs`, `log.rs`, `eos.rs`).

Wire-protocol encoding tests are in:
- `tests/protocol_transaction_test.rs`
- `src/protocol/kafka/tests/transactions.rs`
