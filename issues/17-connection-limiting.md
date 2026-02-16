# Implement Connection Limiting and Idle Timeout

## Priority: High

## Summary

Add connection management features including maximum connection limits, per-IP limits, and idle connection cleanup. This prevents resource exhaustion and improves stability under load.

## Current State

- Connections accepted without limit in `src/server/mod.rs`
- No tracking of connections per source IP
- No idle connection detection or cleanup
- No metrics for connection counts

## Requirements

### Configuration Options

```bash
--max-connections=10000           # Max total connections (default: 10000)
--max-connections-per-ip=100      # Max per source IP (default: 100)
--connection-idle-timeout=600     # Idle timeout in seconds (default: 600)
```

### Implementation Tasks

1. **Connection Tracking**
   ```rust
   struct ConnectionManager {
       total_count: AtomicUsize,
       max_total: usize,
       per_ip_counts: DashMap<IpAddr, usize>,
       max_per_ip: usize,
       connections: DashMap<ConnectionId, ConnectionInfo>,
   }

   struct ConnectionInfo {
       id: ConnectionId,
       peer_addr: SocketAddr,
       connected_at: Instant,
       last_activity: AtomicInstant,
       user: Option<String>,
   }
   ```

2. **Connection Acceptance**
   - Check limits before accepting connection
   - Return appropriate error if limit exceeded
   - Update counters atomically

3. **Idle Connection Reaper**
   - Background task runs every 60 seconds
   - Closes connections idle longer than timeout
   - Logs closed connections at debug level

4. **Connection Lifecycle Hooks**
   ```rust
   impl ConnectionManager {
       pub fn accept(&self, addr: SocketAddr) -> Result<ConnectionId, LimitExceeded>;
       pub fn activity(&self, id: ConnectionId);
       pub fn close(&self, id: ConnectionId);
   }
   ```

### Metrics

```
streamline_connections_active{} - Current active connections
streamline_connections_total{} - Total connections accepted (counter)
streamline_connections_rejected{reason="limit"} - Rejected connections
streamline_connections_closed{reason="idle|client|error"} - Closed connections
streamline_connections_per_ip{ip="x.x.x.x"} - Connections by IP (top N)
```

### Error Response

When connection limit exceeded, server should:
1. Accept the TCP connection (to send error response)
2. Send Kafka error response with appropriate error code
3. Close connection gracefully

### Acceptance Criteria

- [ ] Total connection limit enforced
- [ ] Per-IP limit enforced
- [ ] Idle connections closed after timeout
- [ ] Metrics exposed for monitoring
- [ ] Graceful error response when limit exceeded
- [ ] No race conditions in counter updates
- [ ] Performance impact minimal (< 1% overhead)

## Related Files

- `src/server/mod.rs` - Connection handling
- `src/config/mod.rs` - Configuration
- `src/metrics/mod.rs` - Metrics

## Labels

`high-priority`, `stability`, `production-readiness`
