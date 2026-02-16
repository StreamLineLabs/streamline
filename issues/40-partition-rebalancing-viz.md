# Issue #40: Partition Rebalancing Visualization

## Summary
Add a visualization dashboard showing partition assignment changes during consumer group rebalances, including assignment matrices, rebalance history, and stuck rebalance detection.

## Priority
Low - Phase 3 Enterprise Feature

## Requirements

### 1. Partition Assignment Matrix
- Visual grid showing topic partitions vs consumers
- Color-coded assignment status
- Real-time updates during rebalances
- Filter by topic or consumer group

### 2. Rebalance History Timeline
- Chronological view of rebalance events
- Show trigger reason (member join/leave, heartbeat timeout)
- Duration of each rebalance
- Before/after partition assignment snapshots

### 3. Assignment Diff View
- Side-by-side comparison of assignments
- Highlight moved partitions
- Show reassignment count
- Impact analysis (partitions affected)

### 4. Stuck Rebalance Detection
- Alert when rebalance exceeds threshold
- Show current rebalance state
- List members in pending state
- Recommend actions for resolution

### 5. Consumer Member Details
- List all members in a consumer group
- Show assigned partitions per member
- Client ID and host information
- Session timeout and heartbeat interval

## Technical Implementation

### Types (client.rs)
```rust
pub struct RebalanceEvent {
    pub id: String,
    pub group_id: String,
    pub trigger: RebalanceTrigger,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub generation_id: i32,
    pub previous_generation: Option<i32>,
    pub members_before: i32,
    pub members_after: i32,
    pub partitions_moved: i32,
    pub status: RebalanceStatus,
}

pub enum RebalanceTrigger {
    MemberJoin,
    MemberLeave,
    HeartbeatTimeout,
    TopicMetadataChange,
    Manual,
}

pub enum RebalanceStatus {
    InProgress,
    Completed,
    Failed,
    Stuck,
}

pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub consumer_id: String,
    pub client_id: Option<String>,
    pub host: Option<String>,
}

pub struct ConsumerMember {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assigned_partitions: Vec<TopicPartition>,
    pub session_timeout_ms: i32,
    pub heartbeat_interval_ms: i32,
    pub last_heartbeat: Option<String>,
    pub state: MemberState,
}

pub enum MemberState {
    Stable,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

pub struct AssignmentDiff {
    pub group_id: String,
    pub before_generation: i32,
    pub after_generation: i32,
    pub added: Vec<PartitionAssignment>,
    pub removed: Vec<PartitionAssignment>,
    pub moved: Vec<PartitionMove>,
}

pub struct PartitionMove {
    pub topic: String,
    pub partition: i32,
    pub from_consumer: String,
    pub to_consumer: String,
}

pub struct RebalanceListResponse {
    pub events: Vec<RebalanceEvent>,
    pub total: i32,
}
```

### Routes (routes.rs)
- `GET /rebalances` - Rebalance history page
- `GET /rebalances/:group_id` - Group rebalance details
- `GET /api/rebalances` - List rebalance events
- `GET /api/rebalances/:group_id` - Get group rebalance history
- `GET /api/rebalances/:group_id/current` - Get current rebalance state
- `GET /api/rebalances/:group_id/assignments` - Get current assignments
- `GET /api/rebalances/:group_id/diff/:gen1/:gen2` - Compare generations

### Templates (templates.rs)
- `rebalance_history()` - Overall rebalance history dashboard
- `rebalance_details()` - Specific group rebalance details

### JavaScript (main.js)
- Assignment matrix visualization
- Timeline chart for rebalance history
- Real-time status updates
- Diff highlighting

### CSS (main.css)
- Assignment matrix grid
- Rebalance status indicators
- Timeline styling
- Partition movement animations

## Acceptance Criteria
- [ ] Users can view partition assignment matrix for any consumer group
- [ ] Rebalance history shows all events with timestamps
- [ ] Assignment diff clearly shows moved partitions
- [ ] Stuck rebalances are detected and highlighted
- [ ] Real-time updates during active rebalances

## Files to Modify
- `src/ui/client.rs` - Add rebalance types
- `src/ui/routes.rs` - Add rebalance routes
- `src/ui/templates.rs` - Add rebalance templates
- `src/ui/static/main.css` - Add rebalance styles
