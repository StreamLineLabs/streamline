# Implement Graceful Shutdown with Timeout

## Priority: High

## Summary

Add a configurable timeout for graceful shutdown to ensure the server terminates within a bounded time. Currently, shutdown waits indefinitely for in-flight requests, which could cause deployment issues.

## Current State

- `src/server/mod.rs` has shutdown handling via `ShutdownSignal`
- Cluster nodes attempt leadership transfer on shutdown
- No maximum wait time for graceful period
- No tracking of in-flight requests during shutdown
- Could hang indefinitely if requests don't complete

## Requirements

### Configuration

```bash
--shutdown-timeout=30    # Graceful shutdown timeout in seconds (default: 30)
```

### Shutdown Phases

1. **Stop accepting new connections** (immediate)
   - Close listener socket
   - Log "Shutdown initiated, no longer accepting connections"

2. **Drain in-flight requests** (up to timeout)
   - Track pending requests
   - Wait for completion or timeout
   - Log progress: "Waiting for N requests to complete"

3. **Transfer leadership** (cluster mode, up to timeout/2)
   - Attempt leadership transfer for owned partitions
   - If timeout reached, proceed anyway (new leader will be elected)

4. **Close connections** (after timeout)
   - Send close to remaining connections
   - Allow brief grace period for TCP FIN

5. **Cleanup resources**
   - Flush WAL
   - Close segment files
   - Log final shutdown message

### Implementation Tasks

1. Add `--shutdown-timeout` configuration option
2. Implement request tracking in connection handler
3. Add shutdown state machine with phases
4. Implement timeout enforcement per phase
5. Add shutdown progress logging
6. Ensure WAL flush on shutdown
7. Handle SIGTERM and SIGINT signals
8. Add integration test for shutdown behavior

### Request Tracking

```rust
struct ShutdownCoordinator {
    state: AtomicU8,  // 0=running, 1=draining, 2=closing, 3=stopped
    in_flight: AtomicUsize,
    notify: Notify,
}

impl ShutdownCoordinator {
    pub fn begin_request(&self) -> Option<RequestGuard>;
    pub fn initiate_shutdown(&self);
    pub async fn wait_for_drain(&self, timeout: Duration) -> DrainResult;
}
```

### Logging

```
INFO Received shutdown signal, initiating graceful shutdown
INFO Stopping listener, no new connections accepted
INFO Draining 42 in-flight requests (timeout: 30s)
INFO 10 requests remaining...
WARN Shutdown timeout reached, 3 requests still pending
INFO Transferring partition leadership...
INFO Flushing write-ahead log...
INFO Shutdown complete
```

### Acceptance Criteria

- [ ] Shutdown completes within configured timeout
- [ ] In-flight requests tracked and logged
- [ ] WAL flushed before exit
- [ ] Exit code 0 for clean shutdown, non-zero for timeout
- [ ] SIGTERM and SIGINT both trigger graceful shutdown
- [ ] Second signal forces immediate exit
- [ ] Integration test verifies shutdown behavior

## Related Files

- `src/server/mod.rs` - Server and shutdown handling
- `src/server/shutdown.rs` - Shutdown signal handling
- `src/cluster/mod.rs` - Leadership transfer
- `src/storage/wal.rs` - WAL flush

## Labels

`high-priority`, `stability`, `operations`, `production-readiness`
