//! Policy-as-Code Governance Framework for Streamline
//!
//! This module provides infrastructure-as-code style governance for streaming resources:
//! - Define policies in YAML/JSON files
//! - Version control and GitOps friendly
//! - Audit trail for all policy changes
//! - Declarative configuration for topics, ACLs, quotas, and retention
//!
//! # Example Policy File (streamline-policies.yaml)
//!
//! ```yaml
//! version: "1.0"
//! metadata:
//!   name: production-policies
//!   environment: production
//!
//! topics:
//!   - name: events
//!     partitions: 12
//!     replication_factor: 3
//!     retention:
//!       ms: 604800000  # 7 days
//!       bytes: 10737418240  # 10GB
//!     config:
//!       compression.type: lz4
//!       min.insync.replicas: 2
//!
//! acls:
//!   - principal: "User:alice"
//!     resource_type: topic
//!     resource_name: "events"
//!     operations: [read, write]
//!     permission: allow
//!
//! quotas:
//!   - entity: "User:alice"
//!     producer_byte_rate: 10485760  # 10MB/s
//!     consumer_byte_rate: 20971520  # 20MB/s
//! ```

pub mod audit;
pub mod engine;
pub mod loader;
pub mod types;
pub mod validator;
