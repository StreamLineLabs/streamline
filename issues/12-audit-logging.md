# Implement Security Audit Logging

## Priority: Critical

## Summary

Add comprehensive audit logging for security-critical operations including authentication attempts, authorization decisions, and administrative actions. Essential for security compliance and incident investigation.

## Current State

- Authentication failures logged minimally via `tracing::warn`
- No structured audit log format
- No logging of successful authentications
- ACL authorization decisions not logged
- No correlation IDs for tracking request flows

## Requirements

### Events to Log

#### Authentication Events
- `AUTH_SUCCESS` - Successful authentication (user, mechanism, client IP)
- `AUTH_FAILURE` - Failed authentication (user, mechanism, client IP, reason)
- `AUTH_LOGOUT` - Session termination (user, duration)

#### Authorization Events
- `ACL_ALLOW` - Access granted (user, resource, operation)
- `ACL_DENY` - Access denied (user, resource, operation, reason)

#### Administrative Events
- `TOPIC_CREATE` - Topic created (user, topic, partitions)
- `TOPIC_DELETE` - Topic deleted (user, topic)
- `ACL_CHANGE` - ACL modified (user, change details)
- `CONFIG_CHANGE` - Configuration changed (user, setting, old/new values)

#### Cluster Events
- `NODE_JOIN` - Node joined cluster (node ID, address)
- `NODE_LEAVE` - Node left cluster (node ID, reason)
- `LEADER_CHANGE` - Partition leadership changed (topic, partition, old/new leader)

### Log Format

Structured JSON format for easy parsing:

```json
{
  "timestamp": "2025-01-15T10:30:00.000Z",
  "event_type": "AUTH_FAILURE",
  "correlation_id": "abc-123",
  "client_ip": "192.168.1.100",
  "client_port": 54321,
  "user": "alice",
  "mechanism": "SCRAM-SHA-256",
  "reason": "invalid_password",
  "node_id": 1
}
```

### Configuration

```bash
# Audit logging settings
--audit-log-enabled=true
--audit-log-path=/var/log/streamline/audit.log
--audit-log-format=json  # or "text"
--audit-log-events=all   # or comma-separated list
```

### Implementation Tasks

1. Create `src/audit/mod.rs` module
2. Define `AuditEvent` enum with all event types
3. Implement `AuditLogger` trait with file and stdout backends
4. Add audit hooks to authentication flow (`src/auth/`)
5. Add audit hooks to ACL checks (`src/auth/acl.rs`)
6. Add audit hooks to admin operations
7. Add correlation ID to request context
8. Implement log rotation support

### Acceptance Criteria

- [ ] All authentication attempts logged (success and failure)
- [ ] All ACL decisions logged when audit enabled
- [ ] Administrative operations logged
- [ ] Logs are structured JSON by default
- [ ] Log rotation works (or integrates with logrotate)
- [ ] Performance impact < 1% on throughput
- [ ] Audit log separate from application log

## Related Files

- `src/auth/mod.rs` - Authentication flow
- `src/auth/session.rs` - Session management
- `src/auth/acl.rs` - Authorization checks
- `src/protocol/kafka.rs` - Request handling
- `src/server/mod.rs` - Connection handling

## Labels

`critical`, `security`, `compliance`, `production-readiness`
