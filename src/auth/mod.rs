//! Authentication and Authorization module for Streamline
//!
//! This module provides SASL authentication support, ACL-based authorization,
//! Role-Based Access Control (RBAC), and delegation token management for
//! Kafka protocol compatibility.
//!
//! ## Features
//!
//! - **SASL Authentication**: PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER mechanisms
//! - **OAuth 2.0 / OIDC**: JWT token validation with JWKS support
//! - **ACL Authorization**: Fine-grained Kafka-compatible access control
//! - **RBAC**: Role-based access control with built-in and custom roles
//! - **Delegation Tokens**: Short-lived, revocable credentials for dynamic environments
//!
//! ## Built-in Roles
//!
//! - `cluster-admin`: Full access to all cluster operations
//! - `producer`: Produce messages to topics
//! - `consumer`: Consume messages from topics
//! - `topic-admin`: Create, delete, and configure topics
//! - `readonly`: Read-only access to cluster metadata
//! - `group-admin`: Manage consumer groups

pub mod acl;
pub mod authorizer;
pub mod delegation_token;
pub mod oauth;
pub mod rbac;
pub mod sasl;
pub mod session;
pub mod users;

pub use acl::{Acl, AclFilter, AclStore, Operation, PatternType, Permission, ResourceType};
pub use authorizer::{AclBuilder, AuthorizationResult, Authorizer, AuthorizerConfig};
pub use delegation_token::{
    DelegationToken, DelegationTokenConfig, DelegationTokenManager, DelegationTokenStats,
    TokenCredentials, TokenInfo,
};
pub use oauth::{Audience, OAuthConfig, OAuthProvider, OAuthStats, TokenClaims};
pub use rbac::{RbacManager, Role, RoleBinding, RolePermission};
pub use sasl::{SaslMechanism, SaslPlainAuthenticator, ScramAuthenticator};
pub use session::{AuthSession, SaslSession, ScramState, SessionManager};
pub use users::{ScramCredentials, User, UserStore};
