# Implement Write-Ahead Log (WAL) for Durability

## Summary

Implement a Write-Ahead Log (WAL) to ensure data durability and crash recovery. Currently, data may be lost on crash because writes are buffered and not explicitly fsynced.

## Current State

- `src/config/mod.rs:20-21` - WAL config exists but marked "reserved for future WAL implementation"
- `src/config/mod.rs:87-88` - `wal_sync_ms` field exists but unused
- `src/storage/segment.rs:205-206` - Uses `BufWriter::flush()` which doesn't guarantee fsync
- `src/server/mod.rs:96-100` - Shutdown doesn't flush pending writes
- No crash recovery mechanism exists

## Requirements

### WAL Design

```
data/
├── wal/
│   ├── current.wal        # Active WAL file
│   ├── 00000001.wal       # Archived WAL files
│   └── checkpoint.json    # Last checkpoint state
└── topics/
    └── ...
```

WAL Entry Format:
```rust
pub struct WalEntry {
    pub sequence: u64,           // Monotonic sequence number
    pub timestamp: i64,          // Write timestamp
    pub entry_type: WalEntryType,
    pub topic: String,
    pub partition: i32,
    pub data: Bytes,
    pub crc32: u32,              // Entry checksum
}

pub enum WalEntryType {
    Record,      // Normal record write
    Checkpoint,  // Checkpoint marker
    Truncate,    // Log truncation marker
}
```

### Configuration Options

Update `src/config/mod.rs`:
```rust
pub struct WalConfig {
    /// Enable WAL (default: true)
    pub enabled: bool,
    /// Sync interval in milliseconds (default: 100ms)
    pub sync_interval_ms: u64,
    /// Sync mode: "interval", "every_write", "os_default"
    pub sync_mode: String,
    /// Max WAL file size before rotation (default: 100MB)
    pub max_file_size: u64,
    /// Number of WAL files to retain
    pub retention_files: u32,
}
```

CLI arguments:
- `--wal-enabled` / `STREAMLINE_WAL_ENABLED`
- `--wal-sync-ms` / `STREAMLINE_WAL_SYNC_MS`
- `--wal-sync-mode` / `STREAMLINE_WAL_SYNC_MODE`

### Sync Modes

1. **interval** (default): fsync every N milliseconds (batched for performance)
2. **every_write**: fsync after every produce request (safest, slowest)
3. **os_default**: rely on OS buffer flushing (fastest, least safe)

### Implementation Tasks

1. Create `src/storage/wal.rs`:
   - `WalWriter` - Append entries, handle rotation
   - `WalReader` - Read entries for recovery
   - `WalEntry` serialization/deserialization
2. Create `src/storage/checkpoint.rs`:
   - Track which WAL entries have been applied to segments
   - Periodic checkpointing
3. Modify `src/storage/partition.rs`:
   - Write to WAL before segment
   - Confirm segment write before WAL truncation
4. Modify `src/storage/segment.rs:205`:
   - Replace `flush()` with `sync_all()` or `sync_data()` for fsync
5. Add background sync task in `src/server/mod.rs`
6. Implement crash recovery in `TopicManager::new()`:
   - Read checkpoint
   - Replay WAL entries after checkpoint
   - Rebuild segment state
7. Update graceful shutdown to flush WAL

### Recovery Process

On startup:
1. Load `checkpoint.json` to find last consistent state
2. Scan WAL files for entries after checkpoint
3. Replay uncommitted entries to segments
4. Delete old WAL files
5. Resume normal operation

### Acceptance Criteria

- [ ] WAL written before acknowledge to client
- [ ] Data recoverable after kill -9
- [ ] Configurable fsync interval
- [ ] WAL files rotate at configured size
- [ ] Old WAL files cleaned up after checkpoint
- [ ] Performance benchmarks with/without WAL documented
- [ ] Recovery tested with crash simulation

### Performance Considerations

- Batch fsync calls (group commits)
- Use `O_DIRECT` for WAL writes if beneficial
- Pre-allocate WAL files to avoid fragmentation
- Benchmark: target <10% overhead with interval sync

### Related Files

- `src/config/mod.rs:20-21, 87-88` - Existing WAL config (update)
- `src/storage/segment.rs:200-215` - Segment write (add fsync)
- `src/storage/partition.rs` - Partition append (add WAL step)
- `src/server/mod.rs:96-100` - Shutdown (add WAL flush)

## Labels

`enhancement`, `durability`, `production-readiness`, `large`
