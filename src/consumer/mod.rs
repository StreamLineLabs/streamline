//! Consumer group coordination for Streamline
//!
//! This module implements consumer group support, enabling multiple consumers
//! to coordinate consumption of topic partitions with automatic rebalancing
//! and offset persistence.

pub mod coordinator;
pub mod group;
pub mod kip848;
pub mod lag;
pub mod offset_store;
pub mod rebalance;

pub use coordinator::{
    GroupCoordinator, JoinGroupRequest, JoinGroupRequestBuilder, JoinGroupResponse,
};
pub use group::{CommittedOffset, ConsumerGroup, GroupMember, GroupState};
pub use lag::{ConsumerGroupLag, LagCalculator, LagStats, PartitionLag, TopicLag};
pub use offset_store::OffsetStore;
pub use rebalance::{
    PartitionAssignor, RangeAssignor, RoundRobinAssignor, StickyAssignor, StickyUserData,
    TopicPartition,
};
