# Performance Profiling and Optimization

## Priority: Medium

## Summary

Profile hot paths in storage and protocol handling to identify optimization opportunities. Establish performance baselines and identify any bottlenecks before production release.

## Current State

- Criterion benchmarks exist in `benches/storage_benchmarks.rs`
- No profiling of production workloads
- No established performance baselines
- Lock contention not measured
- Memory allocation patterns unknown

## Requirements

### Profiling Areas

1. **Storage Write Path**
   - Segment append latency
   - Index update overhead
   - WAL sync impact
   - Compression CPU cost

2. **Storage Read Path**
   - Segment lookup time
   - Index search efficiency
   - Decompression overhead
   - Multi-segment reads

3. **Protocol Handling**
   - Request parsing time
   - Response serialization
   - Buffer allocation patterns

4. **Concurrency**
   - Lock contention in TopicManager
   - Partition mutex hold times
   - Async task scheduling overhead

### Profiling Tools

```bash
# CPU profiling with flamegraph
cargo install flamegraph
cargo flamegraph --bin streamline -- --data-dir ./bench-data

# Memory profiling
cargo install cargo-instruments  # macOS
cargo instruments -t Allocations --bin streamline

# Lock contention
# Use tokio-console for async debugging
cargo install tokio-console
```

### Benchmarks to Add

```rust
// benches/protocol_benchmarks.rs
fn bench_produce_request_parse(c: &mut Criterion) { ... }
fn bench_fetch_response_serialize(c: &mut Criterion) { ... }

// benches/concurrent_benchmarks.rs
fn bench_parallel_topic_access(c: &mut Criterion) { ... }
fn bench_partition_contention(c: &mut Criterion) { ... }
```

### Implementation Tasks

1. Run existing benchmarks, document baseline numbers
2. Add protocol parsing/serialization benchmarks
3. Add concurrent access benchmarks
4. Profile with flamegraph under load
5. Identify top 3-5 optimization opportunities
6. Implement optimizations (separate issues)
7. Document performance characteristics

### Potential Optimizations to Investigate

- [ ] Object pooling for request/response buffers
- [ ] Lock-free data structures where applicable
- [ ] Batch multiple small writes
- [ ] Memory-mapped segment files
- [ ] String interning for topic/partition names
- [ ] Zero-copy parsing where possible

### Performance Baselines to Establish

| Metric | Target | Notes |
|--------|--------|-------|
| Single produce latency (p99) | < 5ms | Without fsync |
| Batch produce throughput | > 100K msg/s | 1KB messages |
| Fetch latency (p99) | < 10ms | Warm cache |
| Fetch throughput | > 200 MB/s | Sequential read |
| Connection setup | < 50ms | Including auth |

### Acceptance Criteria

- [ ] Baseline performance documented
- [ ] Flamegraph analysis completed
- [ ] Top bottlenecks identified
- [ ] Benchmark suite expanded
- [ ] No major performance regressions from baseline
- [ ] Performance section added to documentation

## Related Files

- `benches/storage_benchmarks.rs` - Existing benchmarks
- `src/storage/` - Storage layer
- `src/protocol/kafka.rs` - Protocol handling
- `src/storage/topic.rs` - TopicManager (potential contention)

## Labels

`medium-priority`, `performance`, `production-readiness`
