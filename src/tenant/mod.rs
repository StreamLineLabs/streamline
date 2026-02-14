//! Multi-tenancy support for Streamline
//!
//! Provides tenant isolation through namespaces, resource quotas,
//! and tenant-scoped operations. Each tenant gets an isolated view
//! of topics, consumer groups, and other resources.
//!
//! # Design
//!
//! Tenants are identified by a unique `TenantId` string. Resources are
//! namespaced by prefixing topic names with `{tenant_id}.` internally.
//! The tenant layer intercepts requests, applies namespace mapping,
//! enforces quotas, and delegates to the underlying storage/protocol layers.
//!
//! # Feature Gate
//!
//! Multi-tenancy is gated behind the `multi-tenancy` feature flag.

pub mod manager;
pub mod quota;
pub mod types;

pub use manager::TenantManager;
pub use quota::{QuotaConfig, QuotaManager, ResourceUsage};
pub use types::{Tenant, TenantConfig, TenantId, TenantState};
