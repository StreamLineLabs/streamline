//! Consumer group coordination for Kafka-compatible rebalancing.
//!
//! This module implements the server-side consumer group protocol, managing:
//!
//! - **Group membership**: Tracking which consumers belong to each group
//! - **Rebalancing**: Coordinating partition assignment when members join/leave
//! - **Offset management**: Storing and retrieving committed offsets
//! - **Session management**: Detecting failed consumers via heartbeat timeouts
//!
//! # Consumer Group Lifecycle
//!
//! ```text
//! 1. JoinGroup    → Member joins, triggers rebalance
//! 2. SyncGroup    → Leader assigns partitions, members receive assignments
//! 3. Heartbeat    → Members send periodic heartbeats to stay in group
//! 4. OffsetCommit → Members commit their progress
//! 5. LeaveGroup   → Member gracefully leaves (or times out)
//! ```
//!
//! # Rebalancing Protocol
//!
//! When a member joins or leaves:
//! 1. Group enters `PreparingRebalance` state
//! 2. All members must rejoin within `rebalance_timeout_ms`
//! 3. Leader (alphabetically first member) computes partition assignments
//! 4. All members sync and receive their assignments
//! 5. Group enters `Stable` state
//!
//! # Partition Assignment Strategies
//!
//! - **Range** (default): Assigns contiguous partitions per topic to each consumer
//! - **RoundRobin**: Distributes partitions evenly across all consumers
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::consumer::coordinator::{GroupCoordinator, JoinGroupRequest};
//!
//! // Create coordinator
//! let coordinator = GroupCoordinator::new("./offsets", topic_manager)?;
//!
//! // Consumer joins group
//! let request = JoinGroupRequest::builder("my-group", "consumer-1")
//!     .session_timeout_ms(30000)
//!     .subscriptions(vec!["orders".to_string()])
//!     .build();
//!
//! let response = coordinator.join_group_request(request)?;
//! println!("Assigned member ID: {}", response.member_id);
//! println!("Generation: {}", response.generation_id);
//!
//! // Sync to get partition assignments
//! let assignments = coordinator.sync_group(
//!     "my-group",
//!     &response.member_id,
//!     response.generation_id,
//!     HashMap::new(), // Non-leader sends empty map
//! )?;
//!
//! // Start consuming and sending heartbeats...
//! ```
//!
//! # Stability
//!
//! This module is **Stable** - the public API follows semantic versioning.

use crate::consumer::group::{CommittedOffset, ConsumerGroup, GroupMember, GroupState};
use crate::consumer::offset_store::OffsetStore;
use crate::consumer::rebalance::{PartitionAssignor, RangeAssignor, RoundRobinAssignor};
use crate::error::{Result, StreamlineError};
use crate::metrics::{record_group_rebalance, record_rebalance_duration};
use crate::storage::TopicManager;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Request parameters for joining a consumer group
///
/// Use the builder pattern via [`JoinGroupRequestBuilder`] to construct:
///
/// ```ignore
/// let request = JoinGroupRequest::builder("my-group", "my-client")
///     .member_id("existing-member-123")
///     .session_timeout_ms(30000)
///     .rebalance_timeout_ms(60000)
///     .protocol_type("consumer")
///     .protocol_name("range")
///     .subscriptions(vec!["topic1".to_string(), "topic2".to_string()])
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    /// Consumer group ID
    pub group_id: String,
    /// Existing member ID (or None for new members)
    pub member_id: Option<String>,
    /// Client identifier
    pub client_id: String,
    /// Client host address
    pub client_host: String,
    /// Session timeout in milliseconds (default: 30000)
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds (default: 60000)
    pub rebalance_timeout_ms: i32,
    /// Protocol type (default: "consumer")
    pub protocol_type: String,
    /// Protocol name for partition assignment (default: "range")
    pub protocol_name: String,
    /// List of topics to subscribe to
    pub subscriptions: Vec<String>,
    /// Protocol-specific metadata
    pub protocol_metadata: Vec<u8>,
}

impl JoinGroupRequest {
    /// Create a builder for JoinGroupRequest
    pub fn builder(
        group_id: impl Into<String>,
        client_id: impl Into<String>,
    ) -> JoinGroupRequestBuilder {
        JoinGroupRequestBuilder::new(group_id, client_id)
    }
}

/// Builder for [`JoinGroupRequest`]
#[derive(Debug, Clone)]
pub struct JoinGroupRequestBuilder {
    group_id: String,
    member_id: Option<String>,
    client_id: String,
    client_host: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: String,
    protocol_name: String,
    subscriptions: Vec<String>,
    protocol_metadata: Vec<u8>,
}

impl JoinGroupRequestBuilder {
    /// Create a new builder with required fields
    pub fn new(group_id: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: None,
            client_id: client_id.into(),
            client_host: String::new(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            subscriptions: Vec::new(),
            protocol_metadata: Vec::new(),
        }
    }

    /// Set existing member ID (for rejoining)
    pub fn member_id(mut self, member_id: impl Into<String>) -> Self {
        self.member_id = Some(member_id.into());
        self
    }

    /// Set client host address
    pub fn client_host(mut self, host: impl Into<String>) -> Self {
        self.client_host = host.into();
        self
    }

    /// Set session timeout in milliseconds (default: 30000).
    ///
    /// The session timeout is how long a member can go without sending
    /// heartbeats before being considered dead and removed from the group.
    ///
    /// **Typical values:**
    /// - Low latency (fast failure detection): 10000-15000ms
    /// - Default (balanced): 30000ms
    /// - High latency networks: 45000-60000ms
    ///
    /// **Trade-offs:**
    /// - Lower values = faster failure detection, but more false positives
    /// - Higher values = fewer false positives, but slower failure detection
    pub fn session_timeout_ms(mut self, timeout: i32) -> Self {
        self.session_timeout_ms = timeout;
        self
    }

    /// Set rebalance timeout in milliseconds (default: 60000).
    ///
    /// The rebalance timeout is the maximum time allowed for all group members
    /// to rejoin after a rebalance is triggered. Members that don't rejoin
    /// within this time are removed from the group.
    ///
    /// **Typical values:**
    /// - Fast rebalancing: 30000-60000ms
    /// - Default: 60000ms
    /// - Large consumer groups (50+ members): 180000-300000ms
    ///
    /// **Trade-offs:**
    /// - Lower values = faster rebalances, but may drop slow members
    /// - Higher values = more tolerant of slow members, but longer rebalances
    pub fn rebalance_timeout_ms(mut self, timeout: i32) -> Self {
        self.rebalance_timeout_ms = timeout;
        self
    }

    /// Set protocol type
    pub fn protocol_type(mut self, protocol_type: impl Into<String>) -> Self {
        self.protocol_type = protocol_type.into();
        self
    }

    /// Set protocol name for partition assignment (default: "range").
    ///
    /// **Available strategies:**
    /// - `"range"` (default): Assigns contiguous partitions to each consumer.
    ///   Good when partition count is a multiple of consumer count.
    /// - `"roundrobin"`: Distributes partitions evenly across consumers.
    ///   Better distribution when partition count varies.
    ///
    /// # Example
    ///
    /// With 6 partitions and 2 consumers:
    /// - **Range**: Consumer 1 gets [0,1,2], Consumer 2 gets [3,4,5]
    /// - **RoundRobin**: Consumer 1 gets [0,2,4], Consumer 2 gets [1,3,5]
    pub fn protocol_name(mut self, protocol_name: impl Into<String>) -> Self {
        self.protocol_name = protocol_name.into();
        self
    }

    /// Set topics to subscribe to
    pub fn subscriptions(mut self, subscriptions: Vec<String>) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// Add a topic to subscribe to
    pub fn subscribe(mut self, topic: impl Into<String>) -> Self {
        self.subscriptions.push(topic.into());
        self
    }

    /// Set protocol-specific metadata
    pub fn protocol_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.protocol_metadata = metadata;
        self
    }

    /// Build the JoinGroupRequest
    pub fn build(self) -> JoinGroupRequest {
        JoinGroupRequest {
            group_id: self.group_id,
            member_id: self.member_id,
            client_id: self.client_id,
            client_host: self.client_host,
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            protocol_type: self.protocol_type,
            protocol_name: self.protocol_name,
            subscriptions: self.subscriptions,
            protocol_metadata: self.protocol_metadata,
        }
    }
}

/// Response from joining a consumer group.
///
/// Contains the information needed for a consumer to participate in the group:
///
/// - Use `member_id` in subsequent requests (heartbeat, sync, leave)
/// - Use `generation_id` to detect stale requests after rebalances
/// - Compare `member_id == leader_id` to determine if this consumer is the leader
/// - Leaders receive `members` list to compute partition assignments
///
/// # Example
///
/// ```rust,ignore
/// let response = coordinator.join_group_request(request)?;
///
/// // Check if we're the leader
/// if response.member_id == response.leader_id {
///     // Compute and send partition assignments in SyncGroup
///     let assignments = compute_assignments(&response.members, &topics);
///     coordinator.sync_group(group_id, &response.member_id, response.generation_id, assignments)?;
/// } else {
///     // Non-leaders send empty assignments
///     coordinator.sync_group(group_id, &response.member_id, response.generation_id, HashMap::new())?;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// Unique identifier assigned to this consumer within the group.
    ///
    /// This ID is stable across rebalances if the consumer provides its
    /// existing `member_id` when rejoining. Use this ID in all subsequent
    /// requests to the coordinator.
    pub member_id: String,

    /// Monotonically increasing generation counter for this group.
    ///
    /// Incremented each time a rebalance completes. The coordinator rejects
    /// requests with stale generation IDs to prevent race conditions between
    /// old and new consumers during rebalancing.
    pub generation_id: i32,

    /// Member ID of the current group leader.
    ///
    /// The leader is responsible for computing partition assignments during
    /// SyncGroup. Leadership is assigned to the alphabetically-first member
    /// for deterministic selection.
    pub leader_id: String,

    /// List of all member IDs currently in the group.
    ///
    /// Only meaningful for the leader, who uses this to compute partition
    /// assignments. Non-leaders may receive an empty or partial list depending
    /// on the protocol version.
    pub members: Vec<String>,
}

/// Manages consumer groups, partition assignments, and offset storage.
///
/// `GroupCoordinator` is the central component for Kafka-compatible consumer
/// group coordination. It handles:
///
/// - **Group membership**: JoinGroup, LeaveGroup, session timeouts
/// - **Rebalancing**: Partition assignment using Range or RoundRobin strategies
/// - **Offset management**: Commit and fetch offsets for consumer progress tracking
/// - **Persistence**: Saves group state and offsets to disk for crash recovery
///
/// # Thread Safety
///
/// The coordinator is safe to use from multiple threads. It uses `DashMap` for
/// concurrent group access and `RwLock` per group for fine-grained locking.
///
/// # Persistence
///
/// Group metadata and committed offsets are persisted to the `offset_path`
/// directory. On startup, existing groups are restored automatically.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::consumer::coordinator::GroupCoordinator;
/// use streamline::storage::TopicManager;
/// use std::sync::Arc;
///
/// // Create coordinator with offset storage path
/// let topic_manager = Arc::new(TopicManager::new("./data")?);
/// let coordinator = GroupCoordinator::new("./offsets", topic_manager)?;
///
/// // Handle consumer group operations
/// let (member_id, gen, leader, members) = coordinator.join_group(
///     "my-group",           // group_id
///     None,                 // member_id (None = new member)
///     "client-1",           // client_id
///     "127.0.0.1",          // client_host
///     30000,                // session_timeout_ms
///     60000,                // rebalance_timeout_ms
///     "consumer",           // protocol_type
///     "range",              // protocol_name (range or roundrobin)
///     vec!["topic1".into()],// subscriptions
///     vec![],               // protocol_metadata
/// )?;
///
/// // Commit consumer progress
/// coordinator.commit_offset("my-group", "topic1", 0, 100, "".into())?;
///
/// // Fetch committed offset
/// let offset = coordinator.fetch_offset("my-group", "topic1", 0)?;
/// ```
///
/// # Timeout Semantics
///
/// - **Session timeout**: How long a member can go without heartbeats before
///   being considered dead (typically 10-30 seconds)
/// - **Rebalance timeout**: Maximum time allowed for all members to rejoin
///   during a rebalance (typically 60-300 seconds)
pub struct GroupCoordinator {
    /// All consumer groups - uses DashMap for efficient concurrent access
    /// without the lock contention of a nested RwLock pattern
    groups: DashMap<String, Arc<RwLock<ConsumerGroup>>>,

    /// Offset storage for persistent commit tracking
    offset_store: OffsetStore,

    /// Topic manager for partition metadata during rebalancing
    topic_manager: Arc<TopicManager>,
}

impl GroupCoordinator {
    /// Create a new group coordinator with persistent storage.
    ///
    /// Initializes the coordinator and loads any existing consumer groups
    /// from the offset storage directory.
    ///
    /// # Arguments
    ///
    /// * `offset_path` - Directory for storing offset and group state files
    /// * `topic_manager` - Shared reference to topic manager for partition metadata
    ///
    /// # Errors
    ///
    /// Returns an error if the offset storage directory cannot be created or
    /// existing group data cannot be loaded.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let topic_manager = Arc::new(TopicManager::new("./data")?);
    /// let coordinator = GroupCoordinator::new("./offsets", topic_manager)?;
    /// ```
    pub fn new<P: AsRef<Path>>(offset_path: P, topic_manager: Arc<TopicManager>) -> Result<Self> {
        let offset_store = OffsetStore::new(offset_path)?;

        // Load existing groups
        let groups = DashMap::new();
        for group_id in offset_store.list_groups()? {
            if let Some(group) = offset_store.load_group(&group_id)? {
                groups.insert(group_id.clone(), Arc::new(RwLock::new(group)));
            }
        }

        info!(num_groups = groups.len(), "Loaded consumer groups");

        Ok(Self {
            groups,
            offset_store,
            topic_manager,
        })
    }

    /// Get or create a group using DashMap's atomic entry API
    fn get_or_create_group(&self, group_id: &str) -> Arc<RwLock<ConsumerGroup>> {
        self.groups
            .entry(group_id.to_string())
            .or_insert_with(|| {
                debug!(group_id = %group_id, "Creating new consumer group");
                Arc::new(RwLock::new(ConsumerGroup::new(group_id.to_string())))
            })
            .clone()
    }

    /// Handle JoinGroup request using the builder pattern
    ///
    /// # Example
    /// ```ignore
    /// let request = JoinGroupRequest::builder("my-group", "my-client")
    ///     .session_timeout_ms(30000)
    ///     .subscriptions(vec!["topic1".to_string()])
    ///     .build();
    /// let response = coordinator.join_group_request(request)?;
    /// ```
    pub fn join_group_request(&self, request: JoinGroupRequest) -> Result<JoinGroupResponse> {
        let (member_id, generation_id, leader_id, members) = self.join_group_impl(
            &request.group_id,
            request.member_id.as_deref(),
            &request.client_id,
            &request.client_host,
            request.session_timeout_ms,
            request.rebalance_timeout_ms,
            &request.protocol_type,
            &request.protocol_name,
            request.subscriptions,
            request.protocol_metadata,
        )?;

        Ok(JoinGroupResponse {
            member_id,
            generation_id,
            leader_id,
            members,
        })
    }

    /// Handle JoinGroup request (legacy API with many parameters)
    ///
    /// Consider using [`join_group_request`] with [`JoinGroupRequest::builder`] instead.
    #[allow(clippy::too_many_arguments)]
    pub fn join_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        client_id: &str,
        client_host: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocol_name: &str,
        subscriptions: Vec<String>,
        protocol_metadata: Vec<u8>,
    ) -> Result<(String, i32, String, Vec<String>)> {
        self.join_group_impl(
            group_id,
            member_id,
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocol_name,
            subscriptions,
            protocol_metadata,
        )
    }

    /// Internal implementation of join_group
    #[allow(clippy::too_many_arguments)]
    fn join_group_impl(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        client_id: &str,
        client_host: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocol_name: &str,
        subscriptions: Vec<String>,
        protocol_metadata: Vec<u8>,
    ) -> Result<(String, i32, String, Vec<String>)> {
        let group = self.get_or_create_group(group_id);
        let mut group = group.write();

        // Generate or reuse member ID
        let member_id = member_id
            .filter(|id| !id.is_empty())
            .map(|id| id.to_string())
            .unwrap_or_else(|| format!("{}-{}", client_id, uuid::Uuid::new_v4()));

        // Create or update member
        let member = GroupMember {
            member_id: member_id.clone(),
            client_id: client_id.to_string(),
            client_host: client_host.to_string(),
            session_timeout_ms,
            rebalance_timeout_ms,
            subscriptions: subscriptions.clone(),
            assignment: vec![],
            last_heartbeat: Instant::now(),
            protocol_metadata,
        };

        let is_new_member = !group.members.contains_key(&member_id);
        group.members.insert(member_id.clone(), member);

        // Update protocol type and name
        group.protocol_type = protocol_type.to_string();
        group.protocol_name = protocol_name.to_string();

        // Trigger rebalance if needed
        if is_new_member || group.state == GroupState::Empty {
            info!(group_id = %group_id, member_id = %member_id, "Member joining, triggering rebalance");
            group.start_rebalance();
        }

        // Mark member as having joined this rebalance
        group.member_joined(&member_id);

        // Select leader (first member alphabetically for determinism)
        if group.leader.is_none() || is_new_member {
            let mut member_ids: Vec<_> = group.members.keys().cloned().collect();
            member_ids.sort();
            group.leader = member_ids.first().cloned();
        }

        let leader_id = group.leader.clone().unwrap_or_default();
        let generation_id = group.generation_id;

        // Get all member IDs for protocol metadata
        let members: Vec<String> = group.members.keys().cloned().collect();

        // Save group state
        if let Err(e) = self.offset_store.save_group(&group) {
            warn!(error = %e, "Failed to save group state");
        }

        Ok((member_id, generation_id, leader_id, members))
    }

    /// Synchronize group state and receive partition assignments.
    ///
    /// After all members have joined (JoinGroup), they must call SyncGroup:
    /// - The **leader** provides partition assignments for all members
    /// - **Non-leaders** send an empty assignments map
    /// - All members receive their assigned partitions in the response
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group identifier
    /// * `member_id` - This member's ID from JoinGroupResponse
    /// * `generation_id` - Current generation from JoinGroupResponse
    /// * `assignments` - Partition assignments (leader only, others send empty)
    ///
    /// # Returns
    ///
    /// Vector of `(topic, partition)` tuples assigned to this member.
    ///
    /// # Errors
    ///
    /// - `StreamlineError::Protocol` - Generation mismatch (stale request)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After JoinGroup, leader computes and sends assignments
    /// let mut assignments = HashMap::new();
    /// assignments.insert(member1_id, vec![("topic1".into(), 0), ("topic1".into(), 1)]);
    /// assignments.insert(member2_id, vec![("topic1".into(), 2)]);
    ///
    /// // Leader sends assignments
    /// let my_partitions = coordinator.sync_group(
    ///     "my-group",
    ///     &member1_id,
    ///     generation_id,
    ///     assignments,
    /// )?;
    ///
    /// // Non-leader sends empty map
    /// let my_partitions = coordinator.sync_group(
    ///     "my-group",
    ///     &member2_id,
    ///     generation_id,
    ///     HashMap::new(),
    /// )?;
    /// ```
    pub fn sync_group(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        assignments: HashMap<String, Vec<(String, i32)>>,
    ) -> Result<Vec<(String, i32)>> {
        let group = self.get_or_create_group(group_id);
        let mut group = group.write();

        // Verify generation
        if group.generation_id != generation_id {
            return Err(StreamlineError::protocol_msg(format!(
                "Generation mismatch: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        // If this is the leader, apply assignments to all members
        if Some(member_id) == group.leader.as_deref() && !assignments.is_empty() {
            for (mid, assignment) in assignments {
                if let Some(member) = group.members.get_mut(&mid) {
                    member.assignment = assignment;
                }
            }

            group.state = GroupState::Stable;
            info!(group_id = %group_id, generation_id = generation_id, "Group is now stable");

            // Record rebalance metrics
            let duration_ms = group.rebalance_start.elapsed().as_millis() as u64;
            record_group_rebalance(group_id, "sync_complete");
            record_rebalance_duration(group_id, duration_ms);

            // Save group state
            if let Err(e) = self.offset_store.save_group(&group) {
                warn!(error = %e, "Failed to save group state");
            }
        } else if group.state == GroupState::PreparingRebalance {
            // If assignments not provided but state is preparing rebalance, perform auto-assignment
            self.perform_rebalance(&mut group)?;
        }

        // Return this member's assignment
        let assignment = group
            .members
            .get(member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        Ok(assignment)
    }

    /// Perform automatic partition rebalancing
    fn perform_rebalance(&self, group: &mut ConsumerGroup) -> Result<()> {
        // Get all subscribed topics
        let topics = group.get_all_subscribed_topics();

        // Get partition counts for each topic
        let mut topics_partitions = HashMap::new();
        for topic in &topics {
            match self.topic_manager.get_topic_metadata(topic) {
                Ok(metadata) => {
                    topics_partitions.insert(topic.clone(), metadata.num_partitions);
                }
                Err(_) => {
                    warn!(topic = %topic, "Topic not found during rebalance");
                }
            }
        }

        // Choose assignor based on protocol
        let assignments = match group.protocol_name.as_str() {
            "roundrobin" => {
                let assignor = RoundRobinAssignor;
                assignor.assign(&group.members, &topics_partitions)
            }
            _ => {
                // Default to range
                let assignor = RangeAssignor;
                assignor.assign(&group.members, &topics_partitions)
            }
        };

        // Apply assignments
        for (member_id, assignment) in assignments {
            if let Some(member) = group.members.get_mut(&member_id) {
                member.assignment = assignment;
            }
        }

        group.state = GroupState::Stable;

        // Record rebalance metrics
        let duration_ms = group.rebalance_start.elapsed().as_millis() as u64;
        record_group_rebalance(&group.group_id, "auto_assignment");
        record_rebalance_duration(&group.group_id, duration_ms);

        // Save group state
        if let Err(e) = self.offset_store.save_group(group) {
            warn!(error = %e, "Failed to save group state after rebalance");
        }

        Ok(())
    }

    /// Send a heartbeat to keep the member alive in the group.
    ///
    /// Consumers must send heartbeats periodically (typically every 3-10 seconds)
    /// to indicate they are still active. If no heartbeat is received within
    /// `session_timeout_ms`, the member is removed and a rebalance is triggered.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group identifier
    /// * `member_id` - This member's ID from JoinGroupResponse
    /// * `generation_id` - Current generation (must match group's generation)
    ///
    /// # Errors
    ///
    /// - `StreamlineError::Protocol` - Generation mismatch or unknown member
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Send heartbeat every 10 seconds
    /// loop {
    ///     coordinator.heartbeat("my-group", &member_id, generation_id)?;
    ///     tokio::time::sleep(Duration::from_secs(10)).await;
    /// }
    /// ```
    pub fn heartbeat(&self, group_id: &str, member_id: &str, generation_id: i32) -> Result<()> {
        let group = self.get_or_create_group(group_id);
        let mut group = group.write();

        // Verify generation
        if group.generation_id != generation_id {
            return Err(StreamlineError::protocol_msg(format!(
                "Generation mismatch: expected {}, got {}",
                group.generation_id, generation_id
            )));
        }

        // Update heartbeat timestamp
        if let Some(member) = group.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
        } else {
            return Err(StreamlineError::protocol_msg(format!(
                "Unknown member: {}",
                member_id
            )));
        }

        Ok(())
    }

    /// Gracefully remove a member from the consumer group.
    ///
    /// When a consumer shuts down cleanly, it should call LeaveGroup to
    /// immediately trigger a rebalance, rather than waiting for the session
    /// timeout to expire.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group identifier
    /// * `member_id` - This member's ID to remove
    ///
    /// # Behavior
    ///
    /// - Member is removed from the group
    /// - If other members remain, a rebalance is triggered
    /// - If this was the last member, group state becomes `Empty`
    /// - If this was the leader, a new leader is elected
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Clean shutdown
    /// coordinator.leave_group("my-group", &member_id)?;
    /// ```
    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let group = self.get_or_create_group(group_id);
        let mut group = group.write();

        if group.members.remove(member_id).is_some() {
            info!(group_id = %group_id, member_id = %member_id, "Member left group");

            // Trigger rebalance
            if !group.members.is_empty() {
                group.start_rebalance();

                // Reselect leader if needed
                if group.leader.as_deref() == Some(member_id) {
                    let mut member_ids: Vec<_> = group.members.keys().cloned().collect();
                    member_ids.sort();
                    group.leader = member_ids.first().cloned();
                }
            } else {
                group.state = GroupState::Empty;
                group.leader = None;
            }

            // Save group state
            if let Err(e) = self.offset_store.save_group(&group) {
                warn!(error = %e, "Failed to save group state");
            }
        }

        Ok(())
    }

    /// Commit a consumer's progress (offset) for a topic-partition.
    ///
    /// Committed offsets are persisted and survive restarts. When a consumer
    /// joins a group, it can fetch its last committed offset to resume from
    /// where it left off.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group identifier
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `offset` - The offset to commit (typically last processed + 1)
    /// * `metadata` - Optional metadata string (e.g., for debugging)
    ///
    /// # Commit Semantics
    ///
    /// The committed offset should be the **next** offset to read, not the
    /// last processed offset. For example, if you processed offset 99, commit 100.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After processing messages up to offset 99
    /// coordinator.commit_offset(
    ///     "my-group",
    ///     "orders",
    ///     0,           // partition
    ///     100,         // next offset to read
    ///     "".into(),   // metadata
    /// )?;
    /// ```
    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: String,
    ) -> Result<()> {
        let group = self.get_or_create_group(group_id);
        let mut group = group.write();

        let committed_offset = CommittedOffset {
            offset,
            metadata,
            commit_timestamp: chrono::Utc::now().timestamp_millis(),
        };

        group
            .offsets
            .insert((topic.to_string(), partition), committed_offset);

        // Save offsets
        self.offset_store.save_offsets(group_id, &group.offsets)?;

        debug!(
            group_id = %group_id,
            topic = %topic,
            partition = partition,
            offset = offset,
            "Committed offset"
        );

        Ok(())
    }

    /// Fetch the last committed offset for a topic-partition.
    ///
    /// Returns `None` if no offset has been committed for this combination.
    /// Use this when a consumer joins to determine where to start reading.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// match coordinator.fetch_offset("my-group", "orders", 0)? {
    ///     Some(committed) => {
    ///         println!("Resuming from offset {}", committed.offset);
    ///         consumer.seek(committed.offset);
    ///     }
    ///     None => {
    ///         println!("No committed offset, starting from beginning");
    ///         consumer.seek_to_beginning();
    ///     }
    /// }
    /// ```
    pub fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<CommittedOffset>> {
        let group = self.get_or_create_group(group_id);
        let group = group.read();

        Ok(group.offsets.get(&(topic.to_string(), partition)).cloned())
    }

    /// Delete a committed offset
    pub fn delete_offset(&self, group_id: &str, topic: &str, partition: i32) -> Result<()> {
        let group = self.groups.get(group_id).ok_or_else(|| {
            StreamlineError::protocol_msg(format!("Group {} not found", group_id))
        })?;

        let mut group = group.write();
        group.offsets.remove(&(topic.to_string(), partition));

        debug!(group_id, topic, partition, "Deleted committed offset");

        Ok(())
    }

    /// Get group metadata
    pub fn get_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        Ok(self.groups.get(group_id).map(|g| g.read().clone()))
    }

    /// List all groups
    pub fn list_groups(&self) -> Result<Vec<String>> {
        Ok(self.groups.iter().map(|r| r.key().clone()).collect())
    }

    /// Delete a group
    pub fn delete_group(&self, group_id: &str) -> Result<()> {
        // First check if the group exists and is empty
        if let Some(group_ref) = self.groups.get(group_id) {
            let group = group_ref.read();
            if group.state != GroupState::Empty {
                return Err(StreamlineError::protocol_msg(
                    "Cannot delete non-empty group".to_string(),
                ));
            }
            drop(group); // Release read lock before removal
        }

        // Remove the group
        self.groups.remove(group_id);
        self.offset_store.delete_group(group_id)?;
        info!(group_id = %group_id, "Deleted consumer group");

        Ok(())
    }

    /// Save all groups and offsets (for graceful shutdown)
    pub fn save_all(&self) -> Result<()> {
        let mut errors = Vec::new();
        let num_groups = self.groups.len();

        for entry in self.groups.iter() {
            let group_id = entry.key();
            let group = entry.value().read();

            // Save group metadata
            if let Err(e) = self.offset_store.save_group(&group) {
                warn!(group_id = %group_id, error = %e, "Failed to save group during shutdown");
                errors.push(format!("{}: {}", group_id, e));
            }

            // Save offsets
            if let Err(e) = self.offset_store.save_offsets(group_id, &group.offsets) {
                warn!(group_id = %group_id, error = %e, "Failed to save offsets during shutdown");
                errors.push(format!("{} offsets: {}", group_id, e));
            }
        }

        if errors.is_empty() {
            info!(num_groups = num_groups, "Saved all consumer groups");
            Ok(())
        } else {
            Err(StreamlineError::storage_msg(format!(
                "Failed to save some groups: {:?}",
                errors
            )))
        }
    }

    /// Graceful shutdown - marks all members as leaving and saves state
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down consumer group coordinator...");

        for entry in self.groups.iter() {
            let mut group = entry.value().write();

            // Clear all members (they'll reconnect after restart if still active)
            group.members.clear();
            group.pending_members.clear();

            // Set state to empty since there are no members
            group.state = GroupState::Empty;
            group.leader = None;
        }

        // Save all groups
        self.save_all()
    }

    /// Check for expired members and trigger rebalance if needed
    ///
    /// This method processes groups in batches to avoid monopolizing the async
    /// runtime when there are many consumer groups. Between each batch, it yields
    /// to allow other tasks to run.
    pub async fn check_session_timeouts(&self) {
        const BATCH_SIZE: usize = 50;

        let now = Instant::now();

        // Collect all group IDs first to avoid holding iterator across await points
        let group_ids: Vec<String> = self.groups.iter().map(|r| r.key().clone()).collect();

        // Process groups in batches with yield points between batches
        for (batch_idx, chunk) in group_ids.chunks(BATCH_SIZE).enumerate() {
            // Yield between batches (but not before the first batch)
            if batch_idx > 0 {
                tokio::task::yield_now().await;
            }

            for group_id in chunk {
                self.check_group_session_timeout(group_id, now);
            }
        }
    }

    /// Check session timeout for a single group
    fn check_group_session_timeout(&self, group_id: &str, now: Instant) {
        let Some(entry) = self.groups.get(group_id) else {
            return;
        };

        let mut group = entry.write();

        // Check session timeouts for stable groups
        let mut session_expired = Vec::new();
        if group.state == GroupState::Stable {
            for (member_id, member) in &group.members {
                let timeout = std::time::Duration::from_millis(member.session_timeout_ms as u64);
                if now.duration_since(member.last_heartbeat) > timeout {
                    session_expired.push(member_id.clone());
                }
            }
        }

        // Check rebalance timeouts for groups in rebalance state
        let mut rebalance_expired = Vec::new();
        if group.state == GroupState::PreparingRebalance
            || group.state == GroupState::CompletingRebalance
        {
            for (member_id, member) in &group.members {
                // Skip members who have already joined this rebalance
                if group.has_member_joined(member_id) {
                    continue;
                }

                let timeout = std::time::Duration::from_millis(member.rebalance_timeout_ms as u64);
                if now.duration_since(group.rebalance_start) > timeout {
                    rebalance_expired.push(member_id.clone());
                }
            }
        }

        let expired_members: Vec<_> = session_expired
            .into_iter()
            .chain(rebalance_expired.clone())
            .collect();

        if !expired_members.is_empty() {
            if !rebalance_expired.is_empty() {
                warn!(
                    group_id = %group_id,
                    expired = ?rebalance_expired,
                    "Members expired due to rebalance timeout"
                );
            } else {
                warn!(
                    group_id = %group_id,
                    expired = ?expired_members,
                    "Members expired due to session timeout"
                );
            }

            for member_id in &expired_members {
                group.members.remove(member_id);
                group.pending_members.remove(member_id);
            }

            if !group.members.is_empty() {
                group.start_rebalance();
            } else {
                group.state = GroupState::Empty;
                group.leader = None;
            }

            // Save group state
            if let Err(e) = self.offset_store.save_group(&group) {
                warn!(error = %e, "Failed to save group state");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::group::GroupState;
    use tempfile::tempdir;

    /// Helper to create a test coordinator
    fn create_test_coordinator() -> (GroupCoordinator, tempfile::TempDir, tempfile::TempDir) {
        let data_dir = tempdir().unwrap();
        let offset_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(data_dir.path()).unwrap());
        let coordinator = GroupCoordinator::new(offset_dir.path(), topic_manager).unwrap();
        (coordinator, data_dir, offset_dir)
    }

    #[test]
    fn test_join_group() {
        let data_dir = tempdir().unwrap();
        let offset_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(data_dir.path()).unwrap());
        let coordinator = GroupCoordinator::new(offset_dir.path(), topic_manager).unwrap();

        let result = coordinator.join_group(
            "test-group",
            None,
            "client1",
            "localhost",
            45000,
            300000,
            "consumer",
            "range",
            vec!["topic1".to_string()],
            vec![],
        );

        assert!(result.is_ok());
        let (member_id, generation_id, leader_id, _members) = result.unwrap();
        assert!(!member_id.is_empty());
        assert_eq!(generation_id, 1);
        assert_eq!(member_id, leader_id);
    }

    #[test]
    fn test_commit_and_fetch_offset() {
        let data_dir = tempdir().unwrap();
        let offset_dir = tempdir().unwrap();
        let topic_manager = Arc::new(TopicManager::new(data_dir.path()).unwrap());
        let coordinator = GroupCoordinator::new(offset_dir.path(), topic_manager).unwrap();

        coordinator
            .commit_offset("test-group", "topic1", 0, 100, "metadata".to_string())
            .unwrap();

        let offset = coordinator.fetch_offset("test-group", "topic1", 0).unwrap();
        assert!(offset.is_some());
        assert_eq!(offset.unwrap().offset, 100);
    }

    // ==================== sync_group tests ====================

    #[test]
    fn test_sync_group_leader_assigns_partitions() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // First member joins and becomes leader
        let (member1_id, gen_id, leader_id, _) = coordinator
            .join_group(
                "sync-test-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        assert_eq!(member1_id, leader_id);

        // Leader syncs with assignments
        let mut assignments = HashMap::new();
        assignments.insert(member1_id.clone(), vec![("topic1".to_string(), 0)]);

        let assignment = coordinator
            .sync_group("sync-test-group", &member1_id, gen_id, assignments)
            .unwrap();

        assert_eq!(assignment.len(), 1);
        assert_eq!(assignment[0], ("topic1".to_string(), 0));

        // Verify group is stable
        let group = coordinator.get_group("sync-test-group").unwrap().unwrap();
        assert_eq!(group.state, GroupState::Stable);
    }

    #[test]
    fn test_sync_group_generation_mismatch() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group
        let (member_id, gen_id, _, _) = coordinator
            .join_group(
                "gen-mismatch-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Sync with wrong generation
        let result = coordinator.sync_group(
            "gen-mismatch-group",
            &member_id,
            gen_id + 100, // Wrong generation
            HashMap::new(),
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Generation mismatch"));
    }

    // ==================== leave_group tests ====================

    #[test]
    fn test_leave_group_triggers_rebalance() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join two members
        let (member1_id, gen1, _, _) = coordinator
            .join_group(
                "leave-test-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        let (member2_id, gen2, _, _) = coordinator
            .join_group(
                "leave-test-group",
                None,
                "client2",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Second join triggers rebalance, so generation should increase
        assert!(gen2 > gen1);

        // Member 1 leaves
        coordinator
            .leave_group("leave-test-group", &member1_id)
            .unwrap();

        // Check group state - should be preparing rebalance
        let group = coordinator.get_group("leave-test-group").unwrap().unwrap();
        assert_eq!(group.members.len(), 1);
        assert!(group.members.contains_key(&member2_id));
        assert!(!group.members.contains_key(&member1_id));
    }

    #[test]
    fn test_leave_group_last_member_empties_group() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Single member joins
        let (member_id, _, _, _) = coordinator
            .join_group(
                "empty-test-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Member leaves
        coordinator
            .leave_group("empty-test-group", &member_id)
            .unwrap();

        // Group should be empty
        let group = coordinator.get_group("empty-test-group").unwrap().unwrap();
        assert_eq!(group.state, GroupState::Empty);
        assert!(group.members.is_empty());
        assert!(group.leader.is_none());
    }

    // ==================== heartbeat tests ====================

    #[test]
    fn test_heartbeat_success() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group
        let (member_id, gen_id, _, _) = coordinator
            .join_group(
                "heartbeat-test-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Heartbeat should succeed
        let result = coordinator.heartbeat("heartbeat-test-group", &member_id, gen_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_heartbeat_generation_mismatch() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group
        let (member_id, gen_id, _, _) = coordinator
            .join_group(
                "heartbeat-gen-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Heartbeat with wrong generation
        let result = coordinator.heartbeat("heartbeat-gen-group", &member_id, gen_id + 10);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Generation mismatch"));
    }

    #[test]
    fn test_heartbeat_unknown_member() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group to create it
        let (_, gen_id, _, _) = coordinator
            .join_group(
                "heartbeat-unknown-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Heartbeat from unknown member
        let result = coordinator.heartbeat("heartbeat-unknown-group", "unknown-member-id", gen_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown member"));
    }

    // ==================== delete_group tests ====================

    #[test]
    fn test_delete_group_non_empty_fails() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group (creates a non-empty group)
        coordinator
            .join_group(
                "delete-nonempty-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // Try to delete - should fail
        let result = coordinator.delete_group("delete-nonempty-group");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot delete non-empty group"));
    }

    #[test]
    fn test_delete_group_empty_succeeds() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join group then leave
        let (member_id, _, _, _) = coordinator
            .join_group(
                "delete-empty-group",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        coordinator
            .leave_group("delete-empty-group", &member_id)
            .unwrap();

        // Delete should succeed
        let result = coordinator.delete_group("delete-empty-group");
        assert!(result.is_ok());

        // Group should be gone
        let group = coordinator.get_group("delete-empty-group").unwrap();
        assert!(group.is_none());
    }

    // ==================== leader election tests ====================

    #[test]
    fn test_multiple_members_join_deterministic_leader() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join member with ID starting with "z"
        let (member1_id, _, leader1, _) = coordinator
            .join_group(
                "leader-test-group",
                Some("zzz-member"),
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        assert_eq!(member1_id, "zzz-member");
        assert_eq!(leader1, "zzz-member"); // First member is always leader

        // Join member with ID starting with "a" (alphabetically first)
        let (member2_id, _, leader2, _) = coordinator
            .join_group(
                "leader-test-group",
                Some("aaa-member"),
                "client2",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        assert_eq!(member2_id, "aaa-member");
        // Leader should be "aaa-member" (alphabetically first)
        assert_eq!(leader2, "aaa-member");
    }

    // ==================== offset tests ====================

    #[test]
    fn test_fetch_offset_nonexistent() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Fetch offset for topic/partition that was never committed
        let offset = coordinator
            .fetch_offset("nonexistent-group", "nonexistent-topic", 0)
            .unwrap();
        assert!(offset.is_none());
    }

    #[test]
    fn test_delete_offset() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Commit offset
        coordinator
            .commit_offset(
                "offset-delete-group",
                "topic1",
                0,
                100,
                "metadata".to_string(),
            )
            .unwrap();

        // Verify committed
        let offset = coordinator
            .fetch_offset("offset-delete-group", "topic1", 0)
            .unwrap();
        assert!(offset.is_some());

        // Delete offset
        coordinator
            .delete_offset("offset-delete-group", "topic1", 0)
            .unwrap();

        // Verify deleted
        let offset = coordinator
            .fetch_offset("offset-delete-group", "topic1", 0)
            .unwrap();
        assert!(offset.is_none());
    }

    #[test]
    fn test_list_groups() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Initially no groups
        let groups = coordinator.list_groups().unwrap();
        assert!(groups.is_empty());

        // Create some groups by joining
        coordinator
            .join_group(
                "list-group-1",
                None,
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        coordinator
            .join_group(
                "list-group-2",
                None,
                "client2",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        // List groups
        let groups = coordinator.list_groups().unwrap();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"list-group-1".to_string()));
        assert!(groups.contains(&"list-group-2".to_string()));
    }

    #[test]
    fn test_join_group_reuses_member_id() {
        let (coordinator, _data_dir, _offset_dir) = create_test_coordinator();

        // Join with specific member ID
        let (member_id, gen1, _, _) = coordinator
            .join_group(
                "reuse-member-group",
                Some("my-member-id"),
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        assert_eq!(member_id, "my-member-id");

        // Rejoin with same member ID
        let (member_id2, gen2, _, _) = coordinator
            .join_group(
                "reuse-member-group",
                Some("my-member-id"),
                "client1",
                "localhost",
                45000,
                300000,
                "consumer",
                "range",
                vec!["topic1".to_string()],
                vec![],
            )
            .unwrap();

        assert_eq!(member_id2, "my-member-id");
        // Generation should not change on rejoin with same member
        assert_eq!(gen2, gen1);
    }
}
