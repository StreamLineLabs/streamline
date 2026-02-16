# Add Authorization / ACLs

## Summary

Implement authorization controls to restrict which users/clients can perform which operations on which resources. Builds on top of authentication (Issue #2).

## Current State

- No authorization checks
- Any authenticated (or unauthenticated) client can perform any operation
- No ACL storage or management

## Requirements

### ACL Model (Kafka-compatible)

```rust
pub struct Acl {
    pub resource_type: ResourceType,  // Topic, Group, Cluster
    pub resource_name: String,        // topic name or "*" for all
    pub pattern_type: PatternType,    // Literal, Prefixed
    pub principal: String,            // "User:alice" or "User:*"
    pub host: String,                 // "*" or specific IP
    pub operation: Operation,         // Read, Write, Create, Delete, Alter, Describe, All
    pub permission: Permission,       // Allow, Deny
}

pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

pub enum Operation {
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    All,
}
```

### Required Operations by API

| API | Required Operations |
|-----|---------------------|
| Produce | Write on Topic |
| Fetch | Read on Topic |
| CreateTopics | Create on Cluster |
| DeleteTopics | Delete on Topic |
| Metadata | Describe on Topic (or Cluster for all) |
| OffsetCommit | Read on Group |
| JoinGroup | Read on Group |

### Configuration

```yaml
# acls.yaml
acls:
  - resource_type: topic
    resource_name: "*"
    pattern_type: literal
    principal: "User:admin"
    operation: all
    permission: allow

  - resource_type: topic
    resource_name: "events-"
    pattern_type: prefixed
    principal: "User:producer"
    operation: write
    permission: allow
```

### Kafka APIs to Implement

| API | Key | Purpose |
|-----|-----|---------|
| CreateAcls | 30 | Create ACL entries |
| DeleteAcls | 31 | Delete ACL entries |
| DescribeAcls | 29 | List ACL entries |

### Implementation Tasks

1. Create `src/auth/acl.rs`:
   - ACL data structures
   - ACL matching logic
   - ACL storage (file-based initially)

2. Create `src/auth/authorizer.rs`:
   ```rust
   pub struct Authorizer {
       acls: Vec<Acl>,
       default_action: Permission,  // Deny if no ACL matches
   }

   impl Authorizer {
       pub fn authorize(
           &self,
           principal: &str,
           host: &str,
           operation: Operation,
           resource_type: ResourceType,
           resource_name: &str,
       ) -> bool;
   }
   ```

3. Add authorization checks to `src/protocol/kafka.rs`:
   - Before processing Produce: check Write on Topic
   - Before processing Fetch: check Read on Topic
   - etc.

4. Implement ACL management APIs

5. Add CLI commands:
   - `streamline-cli acls add --topic my-topic --principal User:alice --operation read --permission allow`
   - `streamline-cli acls list`
   - `streamline-cli acls delete`

### Super User

Configure super users who bypass ACL checks:
```
--super-users User:admin,User:operator
```

### Acceptance Criteria

- [ ] Unauthorized operations return error code 29 (AUTHORIZATION_FAILED)
- [ ] ACLs can be created/listed/deleted via CLI
- [ ] ACLs can be created/listed/deleted via Kafka API
- [ ] Wildcard matching works ("*" principal, prefixed resources)
- [ ] Super users bypass checks
- [ ] ACLs persisted across restarts
- [ ] kafka-acls.sh tool works

### Related Files

- `src/auth/` - New auth module
- `src/protocol/kafka.rs` - Add auth checks
- `src/cli.rs` - ACL management commands

## Labels

`enhancement`, `security`, `production-readiness`

## Dependencies

Requires Issue #2 (Authentication) to be implemented first.
