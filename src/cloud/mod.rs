//! Streamline Cloud — Managed streaming infrastructure
//!
//! Provides the foundation for running Streamline as a managed service:
//! - Multi-tenant cluster management
//! - Usage metering and billing integration
//! - Automated provisioning and scaling
//! - Health monitoring and alerting
//!
//! Also includes the serverless control plane with:
//! - Instant provisioning (< 30 seconds)
//! - Auto-scaling based on throughput
//! - Zero infrastructure management
//! - Multi-tenant isolation
//! - Usage-based billing
//!
//! # Stability: Experimental
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    STREAMLINE CLOUD                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
//! │  │ API Gateway │  │ Provisioner │  │ Usage Metering          │  │
//! │  │ (REST/gRPC) │  │ (Clusters)  │  │ (Billing Integration)   │  │
//! │  └──────┬──────┘  └──────┬──────┘  └────────────┬────────────┘  │
//! │         │                │                      │                │
//! │         └────────────────┼──────────────────────┘                │
//! │                          ▼                                       │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │                   Control Plane                            │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │  │
//! │  │  │ ClusterMgr   │  │ TenantMgr    │  │ AutoScaler       │ │  │
//! │  │  │ (Kubernetes) │  │ (Isolation)  │  │ (HPA/KEDA)       │ │  │
//! │  │  └──────────────┘  └──────────────┘  └──────────────────┘ │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                          │                                       │
//! │  ┌───────────────────────┼───────────────────────────────────┐  │
//! │  │                 Data Plane                                 │  │
//! │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐    │  │
//! │  │  │ Tenant A    │  │ Tenant B    │  │ Tenant N        │    │  │
//! │  │  │ (Isolated)  │  │ (Isolated)  │  │ (Isolated)      │    │  │
//! │  │  └─────────────┘  └─────────────┘  └─────────────────┘    │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

pub mod api_gateway;
pub mod autoscaler;
pub mod billing;
pub mod cluster_manager;
pub mod console;
pub mod control_plane;
pub mod metering;
pub mod provisioner;
pub mod provisioning;
pub mod serverless;
pub mod tenant;

pub use api_gateway::{ApiGateway, ApiGatewayConfig, RouteConfig};
pub use autoscaler::{AutoScaler, AutoScalerConfig, ScalingDecision, ScalingMetrics};
pub use billing::{BillingEvent, BillingManager, BillingPlan, Invoice, UsageSummary};
pub use cluster_manager::{
    CloudCluster, ClusterConfig, ClusterManager, ClusterState, ClusterStats,
};
pub use console::{
    ApiKey, ApiScope, ConsoleManager, OnboardingRequest, OnboardingResponse, OnboardingStatus,
    TopicUsage, UsageDashboard, UsageDataPoint,
};
pub use control_plane::{
    Alert, AlertSeverity, AlertStatus, AuditEvent, AuditFilter, BillingPlan as OrgBillingPlan,
    ClusterConfig as ManagedClusterConfig, ClusterResources, ClusterStatus, ClusterTier,
    ControlPlane, ControlPlaneConfig, ControlPlaneStatsSnapshot, CreateOrganizationRequest,
    CreateProjectRequest, ManagedCluster, Organization, OrganizationFilter, OrganizationSettings,
    OrganizationStatus, Project, ProjectSettings, ProjectStatus, ProvisionClusterRequest,
    QuotaLimits, QuotaResource, QuotaState, RegionInfo, ResourceUsage, UpdateOrganizationRequest,
};
pub use provisioner::{ProvisionRequest, ProvisionResult, Provisioner, ProvisionerConfig};
pub use serverless::{ServerlessConfig, ServerlessEndpoint, ServerlessInstance, ServerlessManager};

use crate::error::{Result, StreamlineError};
use crate::multitenancy::{Tenant, TenantManager, TenantTier};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cloud platform configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudConfig {
    /// Region for deployment
    pub region: String,
    /// Availability zones
    pub availability_zones: Vec<String>,
    /// Enable auto-scaling
    pub auto_scaling_enabled: bool,
    /// Maximum clusters per tenant
    pub max_clusters_per_tenant: usize,
    /// Default cluster size
    pub default_cluster_size: ClusterSize,
    /// API gateway configuration
    pub api_gateway: ApiGatewayConfig,
    /// Auto-scaler configuration
    pub auto_scaler: AutoScalerConfig,
    /// Provisioner configuration
    pub provisioner: ProvisionerConfig,
    /// Enable billing
    pub billing_enabled: bool,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            availability_zones: vec!["us-east-1a".to_string(), "us-east-1b".to_string()],
            auto_scaling_enabled: true,
            max_clusters_per_tenant: 10,
            default_cluster_size: ClusterSize::Small,
            api_gateway: ApiGatewayConfig::default(),
            auto_scaler: AutoScalerConfig::default(),
            provisioner: ProvisionerConfig::default(),
            billing_enabled: true,
        }
    }
}

/// Cluster size options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ClusterSize {
    /// Development tier - 1 vCPU, 1GB RAM
    Dev,
    /// Small tier - 2 vCPU, 4GB RAM
    #[default]
    Small,
    /// Medium tier - 4 vCPU, 8GB RAM
    Medium,
    /// Large tier - 8 vCPU, 16GB RAM
    Large,
    /// Enterprise tier - 16+ vCPU, 32GB+ RAM
    Enterprise,
}

impl ClusterSize {
    /// Get vCPU count
    pub fn vcpu(&self) -> u32 {
        match self {
            ClusterSize::Dev => 1,
            ClusterSize::Small => 2,
            ClusterSize::Medium => 4,
            ClusterSize::Large => 8,
            ClusterSize::Enterprise => 16,
        }
    }

    /// Get memory in MB
    pub fn memory_mb(&self) -> u32 {
        match self {
            ClusterSize::Dev => 1024,
            ClusterSize::Small => 4096,
            ClusterSize::Medium => 8192,
            ClusterSize::Large => 16384,
            ClusterSize::Enterprise => 32768,
        }
    }

    /// Get storage in GB
    pub fn storage_gb(&self) -> u32 {
        match self {
            ClusterSize::Dev => 10,
            ClusterSize::Small => 50,
            ClusterSize::Medium => 200,
            ClusterSize::Large => 500,
            ClusterSize::Enterprise => 2000,
        }
    }

    /// Get hourly cost in cents
    pub fn hourly_cost_cents(&self) -> u32 {
        match self {
            ClusterSize::Dev => 0, // Free tier
            ClusterSize::Small => 10,
            ClusterSize::Medium => 25,
            ClusterSize::Large => 60,
            ClusterSize::Enterprise => 150,
        }
    }
}

/// Cloud endpoint information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudEndpoint {
    /// Unique endpoint ID
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Bootstrap servers
    pub bootstrap_servers: String,
    /// HTTP API endpoint
    pub http_endpoint: String,
    /// WebSocket endpoint
    pub ws_endpoint: String,
    /// Region
    pub region: String,
    /// Created timestamp
    pub created_at: i64,
    /// Status
    pub status: EndpointStatus,
}

/// Endpoint status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EndpointStatus {
    /// Being provisioned
    #[default]
    Provisioning,
    /// Running and healthy
    Running,
    /// Scaling up or down
    Scaling,
    /// Temporarily unavailable
    Unavailable,
    /// Being terminated
    Terminating,
    /// Terminated
    Terminated,
}

impl std::fmt::Display for EndpointStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EndpointStatus::Provisioning => write!(f, "provisioning"),
            EndpointStatus::Running => write!(f, "running"),
            EndpointStatus::Scaling => write!(f, "scaling"),
            EndpointStatus::Unavailable => write!(f, "unavailable"),
            EndpointStatus::Terminating => write!(f, "terminating"),
            EndpointStatus::Terminated => write!(f, "terminated"),
        }
    }
}

/// Streamline Cloud control plane
pub struct StreamlineCloud {
    /// Configuration
    config: CloudConfig,
    /// Tenant manager
    tenant_manager: Arc<TenantManager>,
    /// Cluster manager
    cluster_manager: Arc<ClusterManager>,
    /// Provisioner
    provisioner: Arc<Provisioner>,
    /// Auto-scaler
    auto_scaler: Arc<AutoScaler>,
    /// API gateway
    api_gateway: Arc<ApiGateway>,
    /// Billing manager
    billing_manager: Arc<BillingManager>,
    /// Active endpoints
    endpoints: Arc<RwLock<HashMap<String, CloudEndpoint>>>,
    /// Control plane for organization/project/cluster management
    control_plane: Arc<ControlPlane>,
}

impl StreamlineCloud {
    /// Create a new Streamline Cloud instance
    pub fn new(config: CloudConfig) -> Result<Self> {
        let tenant_manager = Arc::new(TenantManager::default());
        let cluster_manager = Arc::new(ClusterManager::new(ClusterConfig::default())?);
        let provisioner = Arc::new(Provisioner::new(config.provisioner.clone())?);
        let auto_scaler = Arc::new(AutoScaler::new(config.auto_scaler.clone())?);
        let api_gateway = Arc::new(ApiGateway::new(config.api_gateway.clone())?);
        let billing_manager = Arc::new(BillingManager::new()?);
        let control_plane = Arc::new(ControlPlane::new(ControlPlaneConfig::default()));

        Ok(Self {
            config,
            tenant_manager,
            cluster_manager,
            provisioner,
            auto_scaler,
            api_gateway,
            billing_manager,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            control_plane,
        })
    }

    /// Get a reference to the control plane
    pub fn control_plane(&self) -> &Arc<ControlPlane> {
        &self.control_plane
    }

    /// Create a new serverless endpoint for a tenant
    pub async fn create_endpoint(
        &self,
        tenant_id: &str,
        size: Option<ClusterSize>,
    ) -> Result<CloudEndpoint> {
        // Verify tenant exists
        let tenant = self
            .tenant_manager
            .get_tenant(tenant_id)
            .await
            .ok_or_else(|| StreamlineError::Config(format!("Tenant not found: {}", tenant_id)))?;

        // Check cluster limits
        let endpoints = self.endpoints.read().await;
        let tenant_endpoints = endpoints
            .values()
            .filter(|e| e.tenant_id == tenant_id && e.status != EndpointStatus::Terminated)
            .count();

        if tenant_endpoints >= self.config.max_clusters_per_tenant {
            return Err(StreamlineError::Config(format!(
                "Maximum clusters ({}) reached for tenant",
                self.config.max_clusters_per_tenant
            )));
        }
        drop(endpoints);

        let cluster_size = size.unwrap_or(self.size_for_tier(&tenant.config.tier));

        // Provision the cluster
        let provision_request = ProvisionRequest {
            tenant_id: tenant_id.to_string(),
            size: cluster_size,
            region: self.config.region.clone(),
            availability_zones: self.config.availability_zones.clone(),
        };

        let result = self.provisioner.provision(provision_request).await?;

        let endpoint = CloudEndpoint {
            id: result.cluster_id.clone(),
            tenant_id: tenant_id.to_string(),
            bootstrap_servers: result.bootstrap_servers,
            http_endpoint: result.http_endpoint,
            ws_endpoint: result.ws_endpoint,
            region: self.config.region.clone(),
            created_at: chrono::Utc::now().timestamp_millis(),
            status: EndpointStatus::Running,
        };

        // Store endpoint
        let mut endpoints = self.endpoints.write().await;
        endpoints.insert(endpoint.id.clone(), endpoint.clone());

        // Start billing
        if self.config.billing_enabled {
            self.billing_manager
                .start_metering(tenant_id, &result.cluster_id)
                .await?;
        }

        Ok(endpoint)
    }

    /// Delete an endpoint
    pub async fn delete_endpoint(&self, endpoint_id: &str) -> Result<()> {
        let mut endpoints = self.endpoints.write().await;

        let endpoint = endpoints.get_mut(endpoint_id).ok_or_else(|| {
            StreamlineError::Config(format!("Endpoint not found: {}", endpoint_id))
        })?;

        endpoint.status = EndpointStatus::Terminating;
        let tenant_id = endpoint.tenant_id.clone();
        drop(endpoints);

        // Stop billing
        if self.config.billing_enabled {
            self.billing_manager
                .stop_metering(&tenant_id, endpoint_id)
                .await?;
        }

        // Deprovision
        self.provisioner.deprovision(endpoint_id).await?;

        // Update status
        let mut endpoints = self.endpoints.write().await;
        if let Some(endpoint) = endpoints.get_mut(endpoint_id) {
            endpoint.status = EndpointStatus::Terminated;
        }

        Ok(())
    }

    /// Get endpoint by ID
    pub async fn get_endpoint(&self, endpoint_id: &str) -> Option<CloudEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints.get(endpoint_id).cloned()
    }

    /// List all endpoints for a tenant
    pub async fn list_endpoints(&self, tenant_id: &str) -> Vec<CloudEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints
            .values()
            .filter(|e| e.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Scale an endpoint
    pub async fn scale_endpoint(
        &self,
        endpoint_id: &str,
        new_size: ClusterSize,
    ) -> Result<CloudEndpoint> {
        let mut endpoints = self.endpoints.write().await;

        let endpoint = endpoints.get_mut(endpoint_id).ok_or_else(|| {
            StreamlineError::Config(format!("Endpoint not found: {}", endpoint_id))
        })?;

        endpoint.status = EndpointStatus::Scaling;
        let cluster_id = endpoint.id.clone();
        drop(endpoints);

        // Scale the cluster
        self.cluster_manager
            .scale(&cluster_id, new_size.vcpu(), new_size.memory_mb())
            .await?;

        // Update status
        let mut endpoints = self.endpoints.write().await;
        let endpoint = endpoints.get_mut(endpoint_id).ok_or_else(|| {
            StreamlineError::Config(format!("Endpoint not found: {}", endpoint_id))
        })?;
        endpoint.status = EndpointStatus::Running;

        Ok(endpoint.clone())
    }

    /// Get usage for a tenant
    pub async fn get_usage(&self, tenant_id: &str) -> Result<UsageSummary> {
        self.billing_manager.get_usage(tenant_id).await
    }

    /// Get current invoice for a tenant
    pub async fn get_invoice(&self, tenant_id: &str) -> Result<Invoice> {
        self.billing_manager.get_current_invoice(tenant_id).await
    }

    /// Register a new tenant
    pub async fn register_tenant(
        &self,
        name: &str,
        email: &str,
        tier: TenantTier,
    ) -> Result<Tenant> {
        let tenant_id = uuid::Uuid::new_v4().to_string();
        let mut tenant = Tenant::new(&tenant_id, name).with_tier(tier);
        tenant.contact_email = Some(email.to_string());

        self.tenant_manager
            .create_tenant(tenant.clone())
            .await
            .map_err(|e| StreamlineError::Namespace(e.to_string()))?;

        // Initialize billing account
        if self.config.billing_enabled {
            self.billing_manager
                .create_account(&tenant_id, &tier)
                .await?;
        }

        Ok(tenant)
    }

    /// Map tenant tier to cluster size
    fn size_for_tier(&self, tier: &TenantTier) -> ClusterSize {
        match tier {
            TenantTier::Free => ClusterSize::Dev,
            TenantTier::Basic => ClusterSize::Small,
            TenantTier::Professional => ClusterSize::Medium,
            TenantTier::Enterprise => ClusterSize::Large,
            TenantTier::Custom => self.config.default_cluster_size,
        }
    }

    /// Start the cloud control plane
    pub async fn start(&self) -> Result<()> {
        // Start API gateway
        self.api_gateway.start().await?;

        // Start auto-scaler if enabled
        if self.config.auto_scaling_enabled {
            self.auto_scaler.start().await?;
        }

        tracing::info!(
            region = %self.config.region,
            "Streamline Cloud control plane started"
        );

        Ok(())
    }

    /// Stop the cloud control plane
    pub async fn stop(&self) -> Result<()> {
        self.api_gateway.stop().await?;
        self.auto_scaler.stop().await?;

        tracing::info!("Streamline Cloud control plane stopped");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_size_resources() {
        assert_eq!(ClusterSize::Small.vcpu(), 2);
        assert_eq!(ClusterSize::Small.memory_mb(), 4096);
        assert_eq!(ClusterSize::Dev.hourly_cost_cents(), 0);
    }

    #[test]
    fn test_cloud_config_default() {
        let config = CloudConfig::default();
        assert_eq!(config.region, "us-east-1");
        assert!(config.auto_scaling_enabled);
    }

    #[test]
    fn test_endpoint_status_display() {
        assert_eq!(EndpointStatus::Running.to_string(), "running");
        assert_eq!(EndpointStatus::Provisioning.to_string(), "provisioning");
    }
}
