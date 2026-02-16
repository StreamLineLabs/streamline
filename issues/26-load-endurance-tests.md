# Add Load and Endurance Test Suite

## Priority: Nice to Have

## Summary

Create a comprehensive load and endurance test suite to validate system behavior under sustained load and identify memory leaks, performance degradation, or stability issues over time.

## Current State

- Criterion benchmarks for micro-benchmarks
- Unit and integration tests for correctness
- No sustained load tests
- No endurance tests (long-running)
- No memory leak detection tests

## Requirements

### Load Test Scenarios

1. **Throughput Test**
   - Goal: Find maximum sustainable throughput
   - Duration: 5 minutes
   - Metrics: Messages/sec, latency percentiles

2. **Latency Under Load**
   - Goal: Measure latency at various load levels
   - Levels: 10%, 50%, 80%, 100% of max throughput
   - Metrics: p50, p95, p99, p999 latency

3. **Mixed Workload**
   - Goal: Simulate realistic usage
   - Mix: 70% produce, 30% consume
   - Duration: 10 minutes

4. **Spike Test**
   - Goal: Verify behavior under sudden load increase
   - Pattern: Normal → 10x spike → Normal
   - Metrics: Recovery time, error rate

### Endurance Test Scenarios

1. **24-Hour Stability**
   - Goal: Verify no degradation over time
   - Duration: 24 hours
   - Load: 50% of max throughput
   - Metrics: Latency trend, memory usage, error count

2. **Memory Leak Detection**
   - Goal: Identify memory leaks
   - Duration: 4 hours
   - Pattern: Connect/disconnect cycles, create/delete topics
   - Metrics: RSS memory over time

3. **Segment Rotation Stress**
   - Goal: Verify segment rotation under load
   - Duration: 2 hours
   - Config: Small segment size (1MB)
   - Metrics: Disk usage, rotation latency

### Test Framework

```rust
// load_tests/framework.rs

pub struct LoadTest {
    pub name: String,
    pub duration: Duration,
    pub target_rps: u64,
    pub message_size: usize,
    pub producers: usize,
    pub consumers: usize,
}

pub struct LoadTestResult {
    pub total_messages: u64,
    pub throughput_msg_sec: f64,
    pub throughput_mb_sec: f64,
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub errors: u64,
    pub memory_start: usize,
    pub memory_end: usize,
}
```

### Running Load Tests

```bash
# Run all load tests
cargo test --package streamline-load-tests --release

# Run specific test
cargo test --package streamline-load-tests throughput_test --release

# Run endurance tests (long)
cargo test --package streamline-load-tests endurance --release -- --ignored
```

### Implementation Tasks

1. Create `load_tests/` directory with separate Cargo package
2. Implement load test framework
3. Implement throughput test
4. Implement latency test
5. Implement mixed workload test
6. Implement spike test
7. Implement 24-hour endurance test
8. Implement memory leak detection test
9. Add HTML report generation
10. Document running load tests
11. Add to CI (weekly schedule for endurance)

### CI Integration

```yaml
# Run load tests weekly
load-tests:
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday
  steps:
    - run: cargo test --package streamline-load-tests --release
    - run: cargo test --package streamline-load-tests endurance --release -- --ignored
    - uses: actions/upload-artifact@v3
      with:
        name: load-test-results
        path: target/load-test-results/
```

### Report Format

```
=== Load Test Results ===

Test: Throughput Test
Duration: 5m 0s
Messages: 15,234,567
Throughput: 50,781 msg/s (48.4 MB/s)

Latency:
  p50:  0.8ms
  p95:  2.1ms
  p99:  5.3ms
  p999: 12.7ms

Errors: 0 (0.000%)

Memory:
  Start: 124 MB
  End:   156 MB
  Delta: +32 MB

Status: PASS
```

### Acceptance Criteria

- [ ] Throughput test runs and reports results
- [ ] Latency test measures all percentiles
- [ ] Endurance test runs for 24 hours
- [ ] Memory leak test detects obvious leaks
- [ ] HTML report generated
- [ ] Results archived in CI
- [ ] Documentation for running tests

## Related Files

- `benches/storage_benchmarks.rs` - Existing benchmarks
- `tests/` - Existing tests
- `Cargo.toml` - Workspace configuration

## Labels

`nice-to-have`, `testing`, `performance`, `production-readiness`
