# Implement Consumer Groups

## Summary

Add consumer group support to enable multiple consumers to coordinate consumption of topic partitions with automatic rebalancing and offset persistence.

## Current State

- No consumer group coordination
- Clients manage their own offsets (no server-side offset storage)
- No partition assignment protocol
- `src/protocol/kafka.rs:221-246` - Only 6 APIs implemented, missing group APIs

## Requirements

### Kafka APIs to Implement

Add to `src/protocol/kafka.rs`:

| API | Key | Priority | Purpose |
|-----|-----|----------|---------|
| FindCoordinator | 10 | P0 | Locate group coordinator |
| JoinGroup | 11 | P0 | Join consumer group |
| Heartbeat | 12 | P0 | Maintain group membership |
| LeaveGroup | 13 | P0 | Leave consumer group |
| SyncGroup | 14 | P0 | Synchronize partition assignments |
| OffsetCommit | 8 | P0 | Commit consumer offsets |
| OffsetFetch | 9 | P0 | Fetch committed offsets |
| DescribeGroups | 15 | P1 | Describe group state |
| ListGroups | 16 | P1 | List all groups |
| DeleteGroups | 42 | P2 | Delete consumer groups |

### Data Structures

Create `src/consumer/mod.rs`:

```rust
pub struct ConsumerGroup {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol_type: String,  // "consumer"
    pub protocol_name: String,  // "range" or "roundrobin"
    pub leader: Option<String>,
    pub members: HashMap<String, GroupMember>,
    pub state: GroupState,
    pub offsets: HashMap<(String, i32), CommittedOffset>,  // (topic, partition) -> offset
}

pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub subscriptions: Vec<String>,  // topics
    pub assignment: Vec<(String, i32)>,  // assigned (topic, partition) pairs
    pub last_heartbeat: Instant,
}

pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: String,
    pub commit_timestamp: i64,
}
```

### Offset Storage

Offsets stored in `data/consumer_offsets/`:
```
data/
└── consumer_offsets/
    └── {group_id}/
        └── offsets.json
```

Or use internal topic `__consumer_offsets` (Kafka-compatible approach).

### Group Coordinator

Since Streamline is single-node, the server itself is the coordinator for all groups. The `FindCoordinator` response should return self.

### Implementation Tasks

1. Create module structure:
   - `src/consumer/mod.rs` - Module exports
   - `src/consumer/group.rs` - ConsumerGroup management
   - `src/consumer/coordinator.rs` - Group coordination logic
   - `src/consumer/rebalance.rs` - Partition assignment strategies
   - `src/consumer/offset_store.rs` - Offset persistence
2. Implement partition assignment strategies:
   - Range (default)
   - RoundRobin
3. Implement session timeout / heartbeat expiration
4. Add group state machine (Empty -> PreparingRebalance -> CompletingRebalance -> Stable)
5. Implement all Kafka API handlers listed above
6. Update `handle_api_versions()` to advertise new APIs
7. Add CLI commands: `streamline-cli groups list/describe/delete`
8. Handle graceful shutdown (commit offsets, leave groups)

### Session Management

- Default session timeout: 45 seconds
- Default heartbeat interval: 3 seconds
- Background task to expire inactive members

### Acceptance Criteria

- [ ] Single consumer can join group and consume
- [ ] Multiple consumers in same group get partition assignments
- [ ] Rebalance occurs when consumer joins/leaves
- [ ] Offsets committed and recovered across restarts
- [ ] Session timeout triggers rebalance
- [ ] `kafka-consumer-groups.sh --describe` works
- [ ] Standard Kafka consumer clients (librdkafka) work with groups

### Example Client Usage

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

for message in consumer:
    print(message)
```

### Related Files

- `src/protocol/kafka.rs` - Add 10 new API handlers
- `src/storage/topic.rs` - May need `__consumer_offsets` topic
- `src/cli.rs` - Add group management commands
- `src/server/mod.rs` - Background heartbeat checker task

## Labels

`enhancement`, `feature`, `production-readiness`, `large`
