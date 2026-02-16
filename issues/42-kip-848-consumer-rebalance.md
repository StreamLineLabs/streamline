# Implement KIP-848: Next Generation Consumer Rebalance Protocol

## Summary

Implement KIP-848, the next-generation consumer rebalance protocol that eliminates stop-the-world rebalancing through server-side assignment and incremental reconciliation. This enables 10-20x faster rebalancing and allows consumers to continue processing during group membership changes.

Referenced in PRD at `streamline-prd.md:1269`.

## Current State

Streamline implements the **legacy JoinGroup/SyncGroup protocol**:

- `src/consumer/coordinator.rs:62-134` - JoinGroup handler with synchronous barriers
- `src/consumer/coordinator.rs:137-183` - SyncGroup handler with client-side assignment
- `src/consumer/rebalance.rs` - Client-side assignors (Range, RoundRobin, Sticky)
- `src/protocol/kafka.rs:2823-2907` - JoinGroup API handler
- `src/protocol/kafka.rs:3036-3069` - SyncGroup API handler

### Problems with Current Implementation

1. **Stop-the-world rebalancing**: All consumers pause during JoinGroup/SyncGroup barriers
2. **O(n) latency**: Rebalance time scales linearly with group size
3. **Single slow consumer blocks group**: One delayed member stalls everyone
4. **Client-side complexity**: Leader must compute and distribute assignments
5. **Rebalance storms**: Large groups experience cascading timeouts

## Requirements

### Protocol Overview

KIP-848 replaces the two-phase JoinGroup/SyncGroup protocol with a single continuous `ConsumerGroupHeartbeat` API:

```
┌─────────────────────────────────────────────────────────────────┐
│              LEGACY vs KIP-848 COMPARISON                       │
├────────────────────────────┬────────────────────────────────────┤
│     Legacy Protocol        │        KIP-848 Protocol            │
├────────────────────────────┼────────────────────────────────────┤
│                            │                                    │
│  Consumer ─► JoinGroup     │  Consumer ─► Heartbeat             │
│         ◄── Wait...        │         ◄── Assignment (immediate) │
│         ─► SyncGroup       │         ─► Heartbeat               │
│         ◄── Wait...        │         ◄── Assignment update      │
│         ◄── Assignment     │         ─► Heartbeat               │
│                            │         ...continuous...           │
│  ALL CONSUMERS BLOCKED     │  NO BLOCKING - INCREMENTAL         │
│                            │                                    │
└────────────────────────────┴────────────────────────────────────┘
```

### New API: ConsumerGroupHeartbeat (API Key 68)

```rust
/// ConsumerGroupHeartbeat Request (API Key 68)
pub struct ConsumerGroupHeartbeatRequest {
    /// The group identifier
    pub group_id: String,

    /// The member ID (empty string on first request)
    pub member_id: String,

    /// The member epoch:
    /// - 0: Join the group
    /// - -1: Leave the group
    /// - N: Current epoch (acknowledge previous assignment)
    pub member_epoch: i32,

    /// Instance ID for static membership (optional)
    pub instance_id: Option<String>,

    /// Rack ID for rack-aware assignment (optional)
    pub rack_id: Option<String>,

    /// Rebalance timeout in milliseconds
    pub rebalance_timeout_ms: i32,

    /// Topics the member wants to subscribe to
    pub subscribed_topic_names: Vec<String>,

    /// Regex pattern for topic subscription (optional)
    pub subscribed_topic_regex: Option<String>,

    /// Preferred server-side assignor (optional)
    pub server_assignor: Option<String>,

    /// Partitions currently owned by this member
    pub topic_partitions: Vec<TopicPartitions>,
}

/// ConsumerGroupHeartbeat Response
pub struct ConsumerGroupHeartbeatResponse {
    /// Throttle time in milliseconds
    pub throttle_time_ms: i32,

    /// Error code (0 = success)
    pub error_code: i16,

    /// Error message (optional)
    pub error_message: Option<String>,

    /// Assigned member ID (on first join)
    pub member_id: Option<String>,

    /// Current member epoch
    pub member_epoch: i32,

    /// Recommended heartbeat interval
    pub heartbeat_interval_ms: i32,

    /// Current assignment for this member
    pub assignment: Option<Assignment>,
}

pub struct Assignment {
    /// Error code for this assignment
    pub error: i16,

    /// Partitions fully assigned to this member
    pub assigned_partitions: Vec<TopicPartitions>,

    /// Partitions pending (waiting for another member to revoke)
    pub pending_partitions: Vec<TopicPartitions>,
}

pub struct TopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}
```

### Server-Side Assignors

```rust
/// Trait for server-side partition assignors
pub trait ServerSideAssignor: Send + Sync {
    /// Assignor name (e.g., "uniform", "range")
    fn name(&self) -> &str;

    /// Minimum supported version
    fn minimum_version(&self) -> i16 { 0 }

    /// Maximum supported version
    fn maximum_version(&self) -> i16 { 0 }

    /// Compute assignment for all members
    fn assign(
        &self,
        group_spec: &GroupSpec,
        topic_metadata: &TopicMetadata,
    ) -> GroupAssignment;
}

/// Group specification for assignment
pub struct GroupSpec {
    pub members: HashMap<String, MemberSpec>,
    pub subscription_type: SubscriptionType,
}

pub struct MemberSpec {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub subscribed_topics: Vec<String>,
    pub subscribed_pattern: Option<String>,
}

pub enum SubscriptionType {
    Homogeneous,   // All members subscribe to same topics
    Heterogeneous, // Members have different subscriptions
}
```

### Built-in Assignors

1. **UniformAssignor** (default): Evenly distributes partitions across members
2. **RangeAssignor**: Assigns contiguous partition ranges per topic

### Incremental Reconciliation

The reconciliation process has three phases:

```
Phase 1: REVOCATION
┌─────────────────────────────────────────────────────────────┐
│ Coordinator identifies partitions to move                   │
│ Sends revocation request via heartbeat response             │
│ Member continues processing OTHER partitions                │
│ Member acknowledges revocation in next heartbeat            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
Phase 2: EPOCH ADVANCEMENT
┌─────────────────────────────────────────────────────────────┐
│ Member advances to target epoch after revocation            │
│ Coordinator tracks epoch per member                         │
│ Partition now "free" for reassignment                       │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
Phase 3: ASSIGNMENT
┌─────────────────────────────────────────────────────────────┐
│ Coordinator assigns partition to new member                 │
│ New member receives partition in heartbeat response         │
│ New member begins consuming immediately                     │
└─────────────────────────────────────────────────────────────┘
```

### New Error Codes

| Code | Name | Description |
|------|------|-------------|
| 110 | `FENCED_MEMBER_EPOCH` | Member epoch is stale, must rejoin |
| 111 | `UNSUPPORTED_ASSIGNOR` | Requested assignor not available |
| 112 | `UNRELEASED_INSTANCE_ID` | Static instance still held by another member |
| 113 | `INVALID_REGULAR_EXPRESSION` | Subscription regex is invalid |
| 114 | `STALE_MEMBER_EPOCH` | Member epoch is behind current |

### Configuration Options

```rust
// src/config/mod.rs
pub struct ConsumerProtocolConfig {
    /// Enable KIP-848 protocol support (default: true)
    pub enable_new_protocol: bool,

    /// Session timeout for consumer groups (default: 45000ms)
    pub session_timeout_ms: u64,

    /// Heartbeat interval (default: 5000ms)
    pub heartbeat_interval_ms: u64,

    /// Server-side assignors to enable (default: ["uniform", "range"])
    pub assignors: Vec<String>,

    /// Maximum members per group (default: unlimited)
    pub max_group_size: Option<u32>,

    /// Minimum session timeout allowed (default: 10000ms)
    pub min_session_timeout_ms: u64,

    /// Maximum session timeout allowed (default: 300000ms)
    pub max_session_timeout_ms: u64,
}
```

CLI arguments:
```bash
streamline \
  --consumer-session-timeout-ms 45000 \
  --consumer-heartbeat-interval-ms 5000 \
  --consumer-assignors uniform,range \
  --consumer-max-group-size 1000
```

## Implementation Tasks

### Phase 1: Data Structures and State Management

1. Create `src/consumer/kip848/mod.rs`:
   ```rust
   pub mod heartbeat;
   pub mod member;
   pub mod assignment;
   pub mod reconciliation;
   pub mod server_assignor;
   ```

2. Create `src/consumer/kip848/member.rs`:
   ```rust
   pub struct ConsumerMember {
       pub member_id: String,
       pub instance_id: Option<String>,
       pub member_epoch: i32,
       pub previous_epoch: i32,
       pub state: MemberState,
       pub subscribed_topics: Vec<String>,
       pub assigned_partitions: Vec<TopicPartition>,
       pub pending_partitions: Vec<TopicPartition>,
       pub revoking_partitions: Vec<TopicPartition>,
       pub last_heartbeat: Instant,
       pub session_timeout_ms: u64,
       pub rebalance_timeout_ms: i32,
   }

   pub enum MemberState {
       Joining,      // Initial join, awaiting first assignment
       Stable,       // Has assignment, processing normally
       Reconciling,  // Assignment change in progress
       Leaving,      // Departing group
       Fenced,       // Epoch fenced, must rejoin
   }
   ```

3. Create `src/consumer/kip848/assignment.rs`:
   ```rust
   pub struct ConsumerGroupState {
       pub group_id: String,
       pub group_epoch: i32,
       pub protocol: GroupProtocol,
       pub members: HashMap<String, ConsumerMember>,
       pub target_assignment: TargetAssignment,
       pub current_assignment: CurrentAssignment,
       pub assignor: String,
   }

   pub struct TargetAssignment {
       pub epoch: i32,
       pub assignments: HashMap<String, Vec<TopicPartition>>,
   }

   pub struct CurrentAssignment {
       pub epoch: i32,
       pub assignments: HashMap<String, Vec<TopicPartition>>,
   }
   ```

### Phase 2: ConsumerGroupHeartbeat API Handler

1. Add API key constant in `src/protocol/kafka.rs`:
   ```rust
   // API Key 68
   ConsumerGroupHeartbeat => { ... }
   ```

2. Implement handler:
   ```rust
   async fn handle_consumer_group_heartbeat(
       &self,
       request: ConsumerGroupHeartbeatRequest,
       version: i16,
   ) -> Result<ConsumerGroupHeartbeatResponse> {
       // 1. Validate request
       // 2. Get or create group
       // 3. Process member state based on epoch
       // 4. Trigger assignment if needed
       // 5. Return current assignment
   }
   ```

3. Update `handle_api_versions()` to advertise API key 68

### Phase 3: Server-Side Assignors

1. Create `src/consumer/kip848/server_assignor.rs`:
   ```rust
   pub struct UniformAssignor;

   impl ServerSideAssignor for UniformAssignor {
       fn name(&self) -> &str { "uniform" }

       fn assign(&self, spec: &GroupSpec, metadata: &TopicMetadata) -> GroupAssignment {
           // 1. Collect all partitions
           // 2. Sort members deterministically
           // 3. Distribute partitions evenly
           // 4. Handle heterogeneous subscriptions
       }
   }

   pub struct RangeServerAssignor;

   impl ServerSideAssignor for RangeServerAssignor {
       fn name(&self) -> &str { "range" }

       fn assign(&self, spec: &GroupSpec, metadata: &TopicMetadata) -> GroupAssignment {
           // Per-topic range assignment (similar to client-side)
       }
   }
   ```

2. Create assignor registry:
   ```rust
   pub struct AssignorRegistry {
       assignors: HashMap<String, Arc<dyn ServerSideAssignor>>,
   }

   impl AssignorRegistry {
       pub fn select_assignor(&self, members: &[&ConsumerMember]) -> &dyn ServerSideAssignor;
   }
   ```

### Phase 4: Incremental Reconciliation

1. Create `src/consumer/kip848/reconciliation.rs`:
   ```rust
   pub struct ReconciliationEngine {
       pub fn compute_target_assignment(
           group: &ConsumerGroupState,
           assignor: &dyn ServerSideAssignor,
       ) -> TargetAssignment;

       pub fn compute_member_assignment(
           member: &ConsumerMember,
           target: &TargetAssignment,
           current: &CurrentAssignment,
       ) -> MemberAssignmentDelta;

       pub fn process_heartbeat(
           group: &mut ConsumerGroupState,
           member_id: &str,
           owned_partitions: &[TopicPartition],
       ) -> HeartbeatResult;
   }

   pub struct MemberAssignmentDelta {
       pub to_revoke: Vec<TopicPartition>,
       pub to_assign: Vec<TopicPartition>,
       pub pending: Vec<TopicPartition>,
   }
   ```

2. Implement partition movement tracking:
   ```rust
   pub struct PartitionMovement {
       pub partition: TopicPartition,
       pub from_member: Option<String>,
       pub to_member: String,
       pub state: MovementState,
   }

   pub enum MovementState {
       PendingRevocation,
       Revoked,
       Assigned,
   }
   ```

### Phase 5: Dual Protocol Support

1. Update `src/consumer/coordinator.rs`:
   ```rust
   pub struct GroupCoordinator {
       // Existing fields for legacy protocol
       groups: RwLock<HashMap<String, Arc<RwLock<ConsumerGroup>>>>,

       // New fields for KIP-848
       consumer_groups: RwLock<HashMap<String, Arc<RwLock<ConsumerGroupState>>>>,
       assignor_registry: AssignorRegistry,
       config: ConsumerProtocolConfig,
   }

   impl GroupCoordinator {
       /// Route to appropriate protocol handler
       pub fn get_protocol(&self, group_id: &str) -> GroupProtocol {
           // Check if group exists and which protocol it uses
       }
   }
   ```

2. Implement protocol detection:
   - First request determines protocol
   - Group stays on chosen protocol until empty
   - New groups default to KIP-848 if `enable_new_protocol=true`

### Phase 6: Advanced Features

1. **Static Membership** (`instance_id` support):
   - Preserve assignment across transient disconnects
   - Fencing logic for duplicate instance IDs

2. **Regex Subscriptions**:
   - Pattern matching for topic names
   - Automatic rebalance on new topic creation

3. **Rack-Aware Assignment**:
   - Prefer partition replicas on same rack
   - Pass rack ID through heartbeat

## Acceptance Criteria

### Functionality
- [ ] ConsumerGroupHeartbeat API (key 68) fully implemented
- [ ] Server-side UniformAssignor distributes partitions evenly
- [ ] Server-side RangeAssignor assigns contiguous ranges
- [ ] Incremental assignment without stop-the-world
- [ ] Member epoch tracking and fencing works correctly
- [ ] Partition revocation and reassignment flow works
- [ ] Static membership preserves assignments on reconnect

### Backward Compatibility
- [ ] Legacy JoinGroup/SyncGroup protocol continues working
- [ ] Existing Kafka clients (librdkafka <2.10) work unchanged
- [ ] Groups can coexist using different protocols

### Performance
- [ ] Rebalance time <5s for 10-consumer groups (vs 20-30s legacy)
- [ ] Rebalance time <15s for 100-consumer groups (vs 100s+ legacy)
- [ ] Consumers continue processing during rebalance
- [ ] No stop-the-world pauses

### Client Compatibility
- [ ] librdkafka 2.10+ works with `group.protocol=consumer`
- [ ] kafka-python with KIP-848 support works
- [ ] Java client 4.0+ works with new protocol

### Operations
- [ ] `streamline-cli groups describe` shows protocol type
- [ ] Metrics for rebalance duration, assignment changes
- [ ] Clear error messages for epoch fencing

## Testing

### Unit Tests
- Member state machine transitions
- Server-side assignor algorithms
- Reconciliation logic
- Epoch tracking and fencing

### Integration Tests
- Full heartbeat flow with multiple members
- Member join/leave cycles
- Rebalance during active consumption
- Mixed protocol groups (isolation)

### Performance Tests
- Rebalance latency benchmarks (10, 50, 100 consumers)
- Comparison with legacy protocol
- Throughput during rebalance

### Compatibility Tests
- librdkafka 2.10 with `group.protocol=consumer`
- Java Kafka client 4.0
- Legacy clients (verify no regression)

## Related Files

### Existing (to modify)
- `src/protocol/kafka.rs` - Add ConsumerGroupHeartbeat handler
- `src/consumer/coordinator.rs` - Add dual protocol support
- `src/consumer/group.rs` - Add ConsumerGroupState
- `src/consumer/mod.rs` - Export new modules
- `src/config/mod.rs` - Add configuration options
- `src/error.rs` - Add new error codes
- `src/cli.rs` - Update groups describe command

### New files to create
- `src/consumer/kip848/mod.rs` - Module organization
- `src/consumer/kip848/heartbeat.rs` - Heartbeat request handling
- `src/consumer/kip848/member.rs` - Member state management
- `src/consumer/kip848/assignment.rs` - Assignment tracking
- `src/consumer/kip848/reconciliation.rs` - Incremental reconciliation
- `src/consumer/kip848/server_assignor.rs` - Server-side assignors

## Migration Path

### For Streamline Operators

1. Upgrade Streamline to version with KIP-848 support
2. Enable with `--enable-new-protocol` (default: true)
3. Existing groups continue using legacy protocol
4. New groups automatically use KIP-848

### For Client Applications

1. Upgrade client library (librdkafka 2.10+, Kafka Java 4.0+)
2. Set `group.protocol=consumer` in consumer config
3. No code changes required - API is transparent

## References

- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848:+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-848 Preview Release Notes](https://cwiki.apache.org/confluence/display/KAFKA/The+Next+Generation+of+the+Consumer+Rebalance+Protocol+(KIP-848)+-+Preview+Release+Notes)
- [Confluent Blog: KIP-848 Consumer Rebalance Protocol](https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/)
- [Instaclustr: Rebalance 20x Faster](https://www.instaclustr.com/blog/rebalance-your-apache-kafka-partitions-with-the-next-generation-consumer-rebalance-protocol/)
- [OSO: The New Consumer Rebalance Protocol](https://oso.sh/blog/the-new-consumer-rebalance-protocol-kip-848/)

## Labels

`enhancement`, `consumer-groups`, `protocol`, `performance`, `large`, `kafka-4.0`
