# Add Connection Logging

## Priority: Quick Win

## Summary

Log client connect/disconnect events with IP addresses for operational visibility and debugging.

## Current State

- Connections accepted silently
- No logging of client IPs
- Difficult to debug connection issues
- No visibility into connection patterns

## Implementation

In `src/server/mod.rs`:

```rust
async fn handle_connection(stream: TcpStream, peer_addr: SocketAddr) {
    tracing::info!(
        peer_addr = %peer_addr,
        "Client connected"
    );

    let result = process_connection(stream).await;

    match result {
        Ok(_) => {
            tracing::info!(
                peer_addr = %peer_addr,
                "Client disconnected normally"
            );
        }
        Err(e) => {
            tracing::warn!(
                peer_addr = %peer_addr,
                error = %e,
                "Client disconnected with error"
            );
        }
    }
}
```

### Log Format

```
INFO Client connected peer_addr=192.168.1.100:54321
INFO Client authenticated peer_addr=192.168.1.100:54321 user=alice mechanism=SCRAM-SHA-256
INFO Client disconnected normally peer_addr=192.168.1.100:54321 duration=45s requests=127
WARN Client disconnected with error peer_addr=192.168.1.100:54322 error="connection reset"
```

### Optional: Connection Metadata

```rust
struct ConnectionContext {
    peer_addr: SocketAddr,
    connected_at: Instant,
    user: Option<String>,
    request_count: AtomicU64,
}
```

## Acceptance Criteria

- [ ] Connect events logged with IP
- [ ] Disconnect events logged with IP
- [ ] Errors include reason
- [ ] Log level appropriate (INFO for normal, WARN for errors)
- [ ] Performance impact minimal

## Effort

~1 hour

## Labels

`quick-win`, `observability`, `operations`
