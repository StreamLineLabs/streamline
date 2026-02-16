# Implement Resource Limits and Backpressure

## Priority: Critical

## Summary

Add configurable resource limits to prevent denial-of-service conditions and ensure stable operation under load. Currently, the server has no limits on connections, memory usage, or request queues.

## Current State

- No limit on concurrent connections
- No per-IP connection limits
- No memory usage tracking or limits
- No queue depth limits for produce/fetch requests
- No backpressure mechanism when overloaded
- Unbounded growth possible under sustained load

## Security/Stability Risk

- **DoS vulnerability**: Single client can exhaust server resources
- **Memory exhaustion**: Large messages or many connections can OOM
- **Cascading failures**: Overload can affect all clients

## Requirements

### Connection Limits

```bash
--max-connections=10000              # Total concurrent connections
--max-connections-per-ip=100         # Per source IP limit
--connection-idle-timeout=600000     # Close idle connections (ms)
```

### Memory Limits

```bash
--max-request-size=104857600         # Max single request (100MB)
--max-fetch-bytes=52428800           # Max fetch response (50MB)
--memory-limit-bytes=1073741824      # Total memory budget (1GB)
```

### Queue/Rate Limits

```bash
--max-inflight-requests=1000         # Max pending requests per connection
--produce-rate-limit=10000           # Max produce requests/sec per client
--fetch-rate-limit=1000              # Max fetch requests/sec per client
```

### Implementation Tasks

1. **Connection Limiting**
   - Add connection counter in `src/server/mod.rs`
   - Track connections per IP using `HashMap<IpAddr, usize>`
   - Reject new connections when limit reached (send Kafka error response)
   - Add idle connection reaper task

2. **Request Size Validation**
   - Validate request size before full parsing in `src/protocol/kafka.rs`
   - Return `MESSAGE_TOO_LARGE` error for oversized requests
   - Add size tracking for fetch responses

3. **Memory Tracking**
   - Implement memory budget tracker
   - Track allocations for in-flight requests
   - Apply backpressure when approaching limit

4. **Rate Limiting**
   - Add token bucket rate limiter per client
   - Return `THROTTLING_QUOTA_EXCEEDED` when rate exceeded
   - Include `throttle_time_ms` in responses

5. **Backpressure**
   - Slow down accept loop when overloaded
   - Return retriable errors instead of blocking indefinitely
   - Implement graceful degradation

### Metrics to Add

- `streamline_connections_total` - Current connection count
- `streamline_connections_rejected_total` - Rejected connections
- `streamline_requests_throttled_total` - Throttled requests
- `streamline_memory_used_bytes` - Current memory usage
- `streamline_request_queue_depth` - Pending requests

### Acceptance Criteria

- [ ] Server rejects connections beyond limit with appropriate error
- [ ] Per-IP limits enforced
- [ ] Oversized requests rejected before full parsing
- [ ] Rate limiting works with proper Kafka error codes
- [ ] Metrics exposed for monitoring limits
- [ ] Graceful behavior under overload (no crashes/hangs)
- [ ] Documentation for tuning limits

## Related Files

- `src/server/mod.rs` - Connection handling
- `src/protocol/kafka.rs` - Request processing
- `src/config/mod.rs` - Configuration
- `src/metrics/mod.rs` - Metrics

## Labels

`critical`, `security`, `stability`, `production-readiness`
