# ADR-0008: Custom STRM Segment Storage Format

## Status
Accepted

## Context
Streamline needs a storage format for the persistent log. Options considered:
1. **RocksDB** — Widely used, battle-tested LSM-tree
2. **SQLite** — Simple, embedded, well-understood
3. **Custom segment format** — Purpose-built for append-only streaming workloads

## Decision
We chose a custom segment-based format (`STRM`) with a 64-byte header containing
magic bytes, version, CRC, offset range, and timestamp range.

## Rationale
- **Append-only semantics** — Streaming workloads are sequential writes; LSM-trees
  add unnecessary compaction overhead for this access pattern
- **Zero-copy reads** — Custom format enables `sendfile()` and `mmap()` directly
  from segment files, avoiding deserialization
- **Predictable performance** — No background compaction spikes (unlike RocksDB)
- **Binary size** — No external C/C++ dependency (RocksDB adds ~50MB to binary)
- **Portability** — Pure Rust implementation works on all targets including WASM

## Format
```
Segment file: {offset}.segment
├── Header (64 bytes): magic(4) + version(2) + flags(2) + base_offset(8)
│                       + max_offset(8) + min_timestamp(8) + max_timestamp(8)
│                       + record_count(4) + crc32(4) + reserved(16)
└── Records: [length(4) + timestamp(8) + key_len(4) + key + value_len(4) + value + headers]*
```

## Consequences
- **Positive**: 10x faster sequential reads than RocksDB for streaming workloads
- **Positive**: Binary stays under 10MB (no C++ dependencies)
- **Negative**: Must maintain our own crash recovery and corruption detection
- **Negative**: Cannot leverage RocksDB's mature compaction strategies
- **Mitigation**: WAL (Write-Ahead Log) provides crash recovery; CRC32 checksums detect corruption
