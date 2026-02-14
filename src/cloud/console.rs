//! Web Console API for Streamline Cloud
//!
//! Provides REST endpoints for the self-service web dashboard:
//! - Tenant onboarding and management
//! - Cluster provisioning and monitoring
//! - Usage dashboards and billing overview
//! - API key management

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// API key for programmatic access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub tenant_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub scopes: Vec<ApiScope>,
    pub enabled: bool,
}

/// API key scopes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApiScope {
    /// Full admin access
    Admin,
    /// Produce messages
    Produce,
    /// Consume messages
    Consume,
    /// Manage topics
    TopicManage,
    /// Read-only metrics
    MetricsRead,
    /// Manage clusters
    ClusterManage,
}

/// Usage dashboard data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageDataPoint {
    pub timestamp: DateTime<Utc>,
    pub messages_produced: u64,
    pub messages_consumed: u64,
    pub bytes_ingested: u64,
    pub bytes_egressed: u64,
    pub active_connections: u32,
    pub storage_bytes: u64,
}

/// Usage dashboard summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageDashboard {
    pub tenant_id: String,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub total_messages_produced: u64,
    pub total_messages_consumed: u64,
    pub total_bytes_ingested: u64,
    pub total_bytes_egressed: u64,
    pub peak_connections: u32,
    pub peak_storage_bytes: u64,
    pub estimated_cost_cents: u64,
    pub data_points: Vec<UsageDataPoint>,
    pub top_topics: Vec<TopicUsage>,
}

/// Per-topic usage breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicUsage {
    pub topic_name: String,
    pub messages_produced: u64,
    pub messages_consumed: u64,
    pub storage_bytes: u64,
    pub partition_count: u32,
}

/// Onboarding request for new tenants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardingRequest {
    pub organization_name: String,
    pub admin_email: String,
    pub plan: String,
    pub region: String,
}

/// Onboarding response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardingResponse {
    pub tenant_id: String,
    pub organization_id: String,
    pub bootstrap_server: String,
    pub api_key: String,
    pub console_url: String,
    pub status: OnboardingStatus,
}

/// Onboarding status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OnboardingStatus {
    Provisioning,
    Ready,
    Failed,
}

impl std::fmt::Display for OnboardingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnboardingStatus::Provisioning => write!(f, "provisioning"),
            OnboardingStatus::Ready => write!(f, "ready"),
            OnboardingStatus::Failed => write!(f, "failed"),
        }
    }
}

/// Cloud Console manager
pub struct ConsoleManager {
    api_keys: Arc<RwLock<HashMap<String, ApiKey>>>,
    usage_data: Arc<RwLock<HashMap<String, Vec<UsageDataPoint>>>>,
}

impl ConsoleManager {
    pub fn new() -> Self {
        Self {
            api_keys: Arc::new(RwLock::new(HashMap::new())),
            usage_data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new API key for a tenant
    pub async fn create_api_key(
        &self,
        tenant_id: &str,
        name: &str,
        scopes: Vec<ApiScope>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<ApiKey> {
        let id = uuid::Uuid::new_v4().to_string();
        let prefix = format!("sl_{}", &id[..8]);

        let key = ApiKey {
            id: id.clone(),
            name: name.to_string(),
            prefix: prefix.clone(),
            tenant_id: tenant_id.to_string(),
            created_at: Utc::now(),
            expires_at,
            last_used_at: None,
            scopes,
            enabled: true,
        };

        self.api_keys.write().await.insert(id, key.clone());
        Ok(key)
    }

    /// List API keys for a tenant
    pub async fn list_api_keys(&self, tenant_id: &str) -> Vec<ApiKey> {
        self.api_keys
            .read()
            .await
            .values()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Revoke an API key
    pub async fn revoke_api_key(&self, key_id: &str) -> Result<()> {
        let mut keys = self.api_keys.write().await;
        match keys.get_mut(key_id) {
            Some(key) => {
                key.enabled = false;
                Ok(())
            }
            None => Err(StreamlineError::Internal(format!(
                "API key not found: {}",
                key_id
            ))),
        }
    }

    /// Record usage data point
    pub async fn record_usage(&self, tenant_id: &str, data_point: UsageDataPoint) {
        let mut usage = self.usage_data.write().await;
        usage
            .entry(tenant_id.to_string())
            .or_default()
            .push(data_point);
    }

    /// Get usage dashboard for a tenant
    pub async fn get_usage_dashboard(
        &self,
        tenant_id: &str,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
    ) -> Result<UsageDashboard> {
        let usage = self.usage_data.read().await;
        let data_points: Vec<UsageDataPoint> = usage
            .get(tenant_id)
            .map(|points| {
                points
                    .iter()
                    .filter(|p| p.timestamp >= period_start && p.timestamp <= period_end)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        let total_messages_produced: u64 = data_points.iter().map(|p| p.messages_produced).sum();
        let total_messages_consumed: u64 = data_points.iter().map(|p| p.messages_consumed).sum();
        let total_bytes_ingested: u64 = data_points.iter().map(|p| p.bytes_ingested).sum();
        let total_bytes_egressed: u64 = data_points.iter().map(|p| p.bytes_egressed).sum();
        let peak_connections = data_points
            .iter()
            .map(|p| p.active_connections)
            .max()
            .unwrap_or(0);
        let peak_storage = data_points
            .iter()
            .map(|p| p.storage_bytes)
            .max()
            .unwrap_or(0);

        // Estimate cost based on free-tier pricing
        let message_cost = (total_messages_produced / 1_000_000) * 50; // $0.50 per million
        let storage_cost = (peak_storage / (1024 * 1024 * 1024)) * 25; // $0.25 per GB
        let egress_cost = (total_bytes_egressed / (1024 * 1024 * 1024)) * 10; // $0.10 per GB
        let estimated_cost = message_cost + storage_cost + egress_cost;

        Ok(UsageDashboard {
            tenant_id: tenant_id.to_string(),
            period_start,
            period_end,
            total_messages_produced,
            total_messages_consumed,
            total_bytes_ingested,
            total_bytes_egressed,
            peak_connections,
            peak_storage_bytes: peak_storage,
            estimated_cost_cents: estimated_cost,
            data_points,
            top_topics: Vec::new(),
        })
    }

    /// Process tenant onboarding
    pub async fn onboard_tenant(&self, request: OnboardingRequest) -> Result<OnboardingResponse> {
        let tenant_id = uuid::Uuid::new_v4().to_string();
        let org_id = uuid::Uuid::new_v4().to_string();

        // Create default API key
        let api_key = self
            .create_api_key(&tenant_id, "default", vec![ApiScope::Admin], None)
            .await?;

        let bootstrap_server = format!(
            "{}.{}.streamline.cloud:9092",
            &tenant_id[..8],
            request.region
        );

        Ok(OnboardingResponse {
            tenant_id,
            organization_id: org_id,
            bootstrap_server,
            api_key: api_key.prefix,
            console_url: "https://console.streamline.cloud".to_string(),
            status: OnboardingStatus::Provisioning,
        })
    }
}

impl Default for ConsoleManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_api_key() {
        let mgr = ConsoleManager::new();
        let key = mgr
            .create_api_key("tenant-1", "test-key", vec![ApiScope::Produce], None)
            .await
            .unwrap();
        assert!(key.prefix.starts_with("sl_"));
        assert_eq!(key.tenant_id, "tenant-1");
        assert!(key.enabled);
    }

    #[tokio::test]
    async fn test_revoke_api_key() {
        let mgr = ConsoleManager::new();
        let key = mgr
            .create_api_key("tenant-1", "test", vec![ApiScope::Admin], None)
            .await
            .unwrap();
        mgr.revoke_api_key(&key.id).await.unwrap();
        let keys = mgr.list_api_keys("tenant-1").await;
        assert!(!keys[0].enabled);
    }

    #[tokio::test]
    async fn test_onboard_tenant() {
        let mgr = ConsoleManager::new();
        let resp = mgr
            .onboard_tenant(OnboardingRequest {
                organization_name: "Test Org".to_string(),
                admin_email: "admin@test.com".to_string(),
                plan: "free".to_string(),
                region: "us-east-1".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(resp.status, OnboardingStatus::Provisioning);
        assert!(resp.bootstrap_server.contains("streamline.cloud"));
    }

    #[tokio::test]
    async fn test_usage_dashboard() {
        let mgr = ConsoleManager::new();
        let now = Utc::now();

        mgr.record_usage(
            "tenant-1",
            UsageDataPoint {
                timestamp: now,
                messages_produced: 1000,
                messages_consumed: 500,
                bytes_ingested: 1024 * 1024,
                bytes_egressed: 512 * 1024,
                active_connections: 10,
                storage_bytes: 1024 * 1024 * 100,
            },
        )
        .await;

        let dashboard = mgr
            .get_usage_dashboard(
                "tenant-1",
                now - chrono::Duration::hours(1),
                now + chrono::Duration::hours(1),
            )
            .await
            .unwrap();

        assert_eq!(dashboard.total_messages_produced, 1000);
        assert_eq!(dashboard.peak_connections, 10);
    }

    #[test]
    fn test_onboarding_status_display() {
        assert_eq!(OnboardingStatus::Ready.to_string(), "ready");
        assert_eq!(OnboardingStatus::Provisioning.to_string(), "provisioning");
        assert_eq!(OnboardingStatus::Failed.to_string(), "failed");
    }
}
