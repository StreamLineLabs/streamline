//! Authorization check methods for KafkaHandler
//!
//! Contains all authorization check methods, with both auth-enabled
//! and auth-disabled (stub) implementations.

use crate::error::Result;
use super::{KafkaHandler, Operation, ResourceType};

impl KafkaHandler {
    /// Get a reference to the authorizer (requires auth feature)
    #[cfg(feature = "auth")]
    pub fn authorizer(&self) -> &super::Authorizer {
        &self.authorizer
    }


    /// Check authorization for producing to topics (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_produce_authorization(
        &self,
        principal: &str,
        host: &str,
        topics: &[&str],
    ) -> Result<()> {
        for topic in topics {
            self.authorizer
                .check_authorization(
                    principal,
                    host,
                    Operation::Write,
                    ResourceType::Topic,
                    topic,
                )
                .await?;
        }
        Ok(())
    }

    /// Check authorization for fetching from topics (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_fetch_authorization(
        &self,
        principal: &str,
        host: &str,
        topics: &[&str],
    ) -> Result<()> {
        for topic in topics {
            self.authorizer
                .check_authorization(principal, host, Operation::Read, ResourceType::Topic, topic)
                .await?;
        }
        Ok(())
    }

    /// Check authorization for creating topics (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_create_topic_authorization(
        &self,
        principal: &str,
        host: &str,
    ) -> Result<()> {
        self.authorizer
            .check_authorization(
                principal,
                host,
                Operation::Create,
                ResourceType::Cluster,
                "kafka-cluster",
            )
            .await
    }

    /// Check authorization for deleting topics (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_delete_topic_authorization(
        &self,
        principal: &str,
        host: &str,
    ) -> Result<()> {
        self.authorizer
            .check_authorization(
                principal,
                host,
                Operation::Delete,
                ResourceType::Cluster,
                "kafka-cluster",
            )
            .await
    }

    /// Check authorization for consumer group operations (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_group_authorization(
        &self,
        principal: &str,
        host: &str,
        group_id: &str,
        operation: Operation,
    ) -> Result<()> {
        self.authorizer
            .check_authorization(principal, host, operation, ResourceType::Group, group_id)
            .await
    }

    /// General authorization check (requires auth feature)
    #[cfg(feature = "auth")]
    pub(in crate::protocol) async fn check_authorization(
        &self,
        principal: &str,
        host: &str,
        operation: Operation,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> Result<()> {
        self.authorizer
            .check_authorization(principal, host, operation, resource_type, resource_name)
            .await
    }


    /// Check produce authorization - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_produce_authorization(
        &self,
        _principal: &str,
        _host: &str,
        _topics: &[&str],
    ) -> Result<()> {
        Ok(())
    }

    /// Check fetch authorization - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_fetch_authorization(
        &self,
        _principal: &str,
        _host: &str,
        _topics: &[&str],
    ) -> Result<()> {
        Ok(())
    }

    /// Check create topic authorization - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_create_topic_authorization(
        &self,
        _principal: &str,
        _host: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Check delete topic authorization - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_delete_topic_authorization(
        &self,
        _principal: &str,
        _host: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Check group authorization - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_group_authorization(
        &self,
        _principal: &str,
        _host: &str,
        _group_id: &str,
        _operation: Operation,
    ) -> Result<()> {
        Ok(())
    }

    /// General authorization check - always allowed when auth is disabled
    #[cfg(not(feature = "auth"))]
    pub(in crate::protocol) async fn check_authorization(
        &self,
        _principal: &str,
        _host: &str,
        _operation: Operation,
        _resource_type: ResourceType,
        _resource_name: &str,
    ) -> Result<()> {
        Ok(())
    }
}
