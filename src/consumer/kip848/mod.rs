//! KIP-848 Next-Generation Consumer Rebalancing Protocol
//!
//! This module implements the server-side components of KIP-848, which introduces
//! a new consumer rebalance protocol with the following key features:
//!
//! - **Incremental Rebalancing**: Partition moves happen incrementally with minimal disruption
//! - **Server-Side Assignment**: The broker computes partition assignments, not the consumer
//! - **Epoch-Based Coordination**: Group and member epochs track state changes
//! - **Simpler Client Protocol**: Single ConsumerGroupHeartbeat API replaces JoinGroup/SyncGroup/Heartbeat
//!
//! ## Key Concepts
//!
//! - **ConsumerGroup**: Represents a group using the new protocol (group.epoch > 0)
//! - **ConsumerMember**: A member of a KIP-848 group with its own member.epoch
//! - **TargetAssignment**: The desired partition assignment computed by the server
//! - **CurrentAssignment**: The partitions a member currently owns
//! - **Reconciliation**: The process of moving from current to target assignment
//!
//! ## State Machine
//!
//! ```text
//! Member States:
//!   Joining -> Stable <-> Reconciling -> Leaving -> Fenced
//!
//! Group States:
//!   Empty -> Stable -> Reconciling -> Empty
//! ```

pub mod assignment;
pub mod errors;
pub mod group;
pub mod heartbeat;
pub mod member;
pub mod reconciliation;
pub mod server_assignor;

pub use assignment::{CurrentAssignment, TargetAssignment, TopicPartitions};
pub use errors::Kip848Error;
pub use group::{ConsumerGroupKip848, GroupEpoch, GroupStateKip848};
pub use heartbeat::{HeartbeatRequest, HeartbeatResponse};
pub use member::{ConsumerMember, MemberEpoch, MemberState};
pub use reconciliation::ReconciliationEngine;
pub use server_assignor::{ServerSideAssignor, UniformAssignor};
