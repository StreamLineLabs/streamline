//! Kafka protocol API handlers
//!
//! This module organizes Kafka API handlers into logical groups:
//! - `error_codes` - Kafka protocol error codes
//! - `core` - ApiVersions, Metadata handlers
//! - `produce` - Produce request handling
//! - `fetch` - Fetch request handling
//! - `topics` - Topic management (CreateTopics, DeleteTopics, etc.)
//! - `consumer_groups` - Consumer group coordination
//! - `offsets` - Offset management
//! - `auth` - SASL authentication handlers
//! - `acls` - ACL management
//! - `transactions` - Transaction handling
//! - `admin` - Administrative operations

mod admin;
mod configs;
mod core;
mod data_plane;
#[allow(dead_code)]
pub mod error_codes;
mod groups;
mod registry;
mod security;
mod topics;
mod transactions;

// Re-export all error codes for convenience
pub use error_codes::*;
pub(crate) use registry::{dispatch_request, DispatchResult};
