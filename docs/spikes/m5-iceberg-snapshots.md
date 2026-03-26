# M5 Spike — Streamline Branches ↔ Iceberg Snapshots/Refs

> Deliverable for `m5-p0-iceberg-snapshots`. Maps Streamline's branch
> model (ADR-0020) onto Apache Iceberg's branching semantics so a
> Streamline branch can be materialized as an Iceberg branch with zero
> data copy.

## 1. Side-by-side

| Concept              | Streamline (ADR-0020)        | Iceberg                       |
|----------------------|------------------------------|-------------------------------|
| Linear history       | Topic / Partition / Segment  | Table / Snapshot              |
| Position cursor      | `offset`                     | `snapshot_id`                 |
| Branch point         | `(topic, partition, base_offset)` | `parent_snapshot_id`     |
| Branch head          | `BranchManifest`             | Named branch ref              |
| Owned writes         | `Owned` segment refs         | New snapshots since branch    |
| Inherited reads      | `Inherited` segment refs     | Walking parent snapshot chain |
| Garbage collection   | Refcount per parent segment  | `expire_snapshots`            |

Both are manifest-based and copy-on-write. The mapping is clean.

## 2. Proposed mapping (M5 P3 work)

```
Streamline topic           Iceberg table
   |                            |
   +-- main (default ref)  -->  main branch
   |                            |
   +-- branch B at offset N --> Iceberg branch B
        (manifest refs:                |
         Inherited[0..N]              parent ref = main@snapshot(N)
         Owned[N..M])                 new snapshots = M-N
```

Algorithm per branch creation:

1. Resolve Iceberg snapshot containing data through Streamline offset N.
2. `manage_snapshots().create_branch("B").with_snapshot_id(s)`.
3. Future commits on branch B → snapshots tagged with branch ref B.
4. `streamline branch discard B` → `drop_branch("B")` + Streamline
   refcount decrement.

## 3. Open Questions

1. **Snapshot granularity.** Streamline batches don't always 1:1 with
   Iceberg snapshots. Need `__branches.iceberg_map` mapping
   `(snapshot_id, base_offset, end_offset)`. Build lazily on first read.
2. **Catalog choice.** REST only in v1 (`iceberg-catalog-rest` already
   in `Cargo.toml`); HMS / Glue tracked behind upstream re-enable.
3. **Schema drift on a branch.** Iceberg requires schema-evolution
   snapshots; in P1 reject schema edits on branches (FR-NN). Branch-level
   schema evolution is M5 P3.
4. **Cleanup coupling.** `expire_snapshots` cannot delete data still
   referenced by a Streamline branch. Register a hook with the Iceberg
   writer (path TBD: `streamline/src/sink/iceberg_writer.rs`) that
   contributes live-snapshot IDs to retention.

## 4. Bibliography

- Iceberg branching: <https://iceberg.apache.org/docs/latest/branching/>
- `org.apache.iceberg.SnapshotRef` — Java API
- `iceberg-catalog-rest` v0.x in `Cargo.toml`
- Internal: ADR-0020; `MOONSHOT_PLAN.md` M5

## 5. Conclusion

Mapping is feasible without new abstractions on the Streamline side.
Risk #1 (snapshot granularity) is the main implementation challenge.
M5 P3 ships:

- `__branches.iceberg_map` system topic with offset→snapshot index.
- `streamline branch sync-iceberg <name>` admin command.
- Property tests in `streamline/tests/branches_iceberg.rs`.
