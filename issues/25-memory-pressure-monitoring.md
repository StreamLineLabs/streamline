# Add Memory Pressure Monitoring

## Priority: Nice to Have

## Summary

Add memory usage tracking and pressure monitoring to prevent OOM conditions and enable proactive alerting. Currently, there's no visibility into memory consumption patterns.

## Current State

- No memory usage tracking
- No metrics for memory consumption
- No backpressure based on memory
- OOM possible under heavy load

## Requirements

### Memory Metrics

```
# Process memory usage
streamline_process_memory_bytes{type="resident"}
streamline_process_memory_bytes{type="virtual"}

# Component memory estimates
streamline_memory_bytes{component="connections"}
streamline_memory_bytes{component="request_buffers"}
streamline_memory_bytes{component="segment_cache"}
streamline_memory_bytes{component="index_cache"}

# Memory pressure state
streamline_memory_pressure{level="normal|warning|critical"}
```

### Memory Budget Configuration

```bash
--memory-limit=1073741824      # 1GB memory budget
--memory-warning-threshold=0.8  # Warn at 80%
--memory-critical-threshold=0.9 # Critical at 90%
```

### Pressure Response Actions

1. **Warning level (80%)**
   - Log warning
   - Emit metric
   - Start reducing caches

2. **Critical level (90%)**
   - Log critical
   - Emit metric
   - Reject new connections
   - Apply aggressive backpressure
   - Evict caches

3. **Recovery**
   - Resume normal operation when below warning

### Implementation Tasks

1. Add memory tracking infrastructure
2. Implement process memory reading (platform-specific)
3. Add per-component memory estimation
4. Implement memory pressure state machine
5. Add backpressure responses
6. Add metrics
7. Add configuration options
8. Test pressure responses

### Memory Tracking Approach

```rust
pub struct MemoryTracker {
    limit: usize,
    warning_threshold: f64,
    critical_threshold: f64,
    current_usage: AtomicUsize,
    pressure_level: AtomicU8,
}

impl MemoryTracker {
    pub fn allocate(&self, size: usize) -> Result<MemoryGuard, MemoryPressure>;
    pub fn current_usage(&self) -> usize;
    pub fn pressure_level(&self) -> PressureLevel;
}

pub struct MemoryGuard {
    tracker: Arc<MemoryTracker>,
    size: usize,
}

impl Drop for MemoryGuard {
    fn drop(&mut self) {
        self.tracker.deallocate(self.size);
    }
}
```

### Platform-Specific Memory Reading

```rust
#[cfg(target_os = "linux")]
fn get_process_memory() -> MemoryStats {
    // Read from /proc/self/statm
}

#[cfg(target_os = "macos")]
fn get_process_memory() -> MemoryStats {
    // Use mach_task_basic_info
}
```

### Acceptance Criteria

- [ ] Memory metrics exposed
- [ ] Pressure levels detected correctly
- [ ] Backpressure applied at critical level
- [ ] Recovery when pressure relieved
- [ ] Works on Linux and macOS
- [ ] Minimal overhead (< 1%)
- [ ] Documentation updated

## Related Files

- `src/metrics/mod.rs` - Metrics
- `src/server/mod.rs` - Connection handling
- `src/config/mod.rs` - Configuration

## Labels

`nice-to-have`, `stability`, `observability`
