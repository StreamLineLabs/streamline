//! Streamline Cloud Control Plane
//!
//! Provides the centralized management plane for Streamline Cloud SaaS:
//! - Organization and project management
//! - Cluster lifecycle management
//! - Multi-region orchestration
//! - Quota and resource management
//! - Audit and compliance logging
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                       STREAMLINE CLOUD CONTROL PLANE                        │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  ┌───────────────────────────────────────────────────────────────────────┐  │
//! │  │                         API Layer                                     │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────┐│  │
//! │  │  │ REST API     │  │ gRPC API     │  │ WebSocket (Real-time)       ││  │
//! │  │  │ /api/v1/*    │  │ StreamlineCP │  │ Events, Metrics, Logs       ││  │
//! │  │  └──────────────┘  └──────────────┘  └──────────────────────────────┘│  │
//! │  └───────────────────────────────────────────────────────────────────────┘  │
//! │                                    │                                        │
//! │  ┌───────────────────────────────────────────────────────────────────────┐  │
//! │  │                    Core Services Layer                                │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │  │
//! │  │  │ OrganizationMgr│  │ ProjectMgr   │  │ ClusterMgr   │  │ QuotaMgr│ │  │
//! │  │  │ (IAM, RBAC)  │  │ (Namespaces) │  │ (Lifecycle)  │  │ (Limits)│ │  │
//! │  │  └──────────────┘  └──────────────┘  └──────────────┘  └───────────┘ │  │
//! │  └───────────────────────────────────────────────────────────────────────┘  │
//! │                                    │                                        │
//! │  ┌───────────────────────────────────────────────────────────────────────┐  │
//! │  │                    Platform Services                                  │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │  │
//! │  │  │ RegionMgr    │  │ AuditLog     │  │ AlertMgr     │  │ BackupMgr│ │  │
//! │  │  │ (Multi-Cloud)│  │ (Compliance) │  │ (PagerDuty)  │  │ (DR)    │ │  │
//! │  │  └──────────────┘  └──────────────┘  └──────────────┘  └───────────┘ │  │
//! │  └───────────────────────────────────────────────────────────────────────┘  │
//! │                                                                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Control plane manager
pub struct ControlPlane {
    config: ControlPlaneConfig,
    organizations: Arc<RwLock<HashMap<String, Organization>>>,
    projects: Arc<RwLock<HashMap<String, Project>>>,
    clusters: Arc<RwLock<HashMap<String, ManagedCluster>>>,
    quotas: Arc<RwLock<HashMap<String, QuotaState>>>,
    audit_log: Arc<RwLock<Vec<AuditEvent>>>,
    alerts: Arc<RwLock<Vec<Alert>>>,
    stats: Arc<ControlPlaneStats>,
}

impl ControlPlane {
    /// Create a new control plane
    pub fn new(config: ControlPlaneConfig) -> Self {
        Self {
            config,
            organizations: Arc::new(RwLock::new(HashMap::new())),
            projects: Arc::new(RwLock::new(HashMap::new())),
            clusters: Arc::new(RwLock::new(HashMap::new())),
            quotas: Arc::new(RwLock::new(HashMap::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
            alerts: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(ControlPlaneStats::default()),
        }
    }

    // ==================== Organization Management ====================

    /// Create a new organization
    pub async fn create_organization(
        &self,
        request: CreateOrganizationRequest,
    ) -> Result<Organization> {
        let org_id = uuid::Uuid::new_v4().to_string();

        let org = Organization {
            id: org_id.clone(),
            name: request.name,
            slug: request.slug,
            owner_id: request.owner_id,
            plan: request.plan,
            status: OrganizationStatus::Active,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            settings: request.settings.unwrap_or_default(),
            metadata: request.metadata.unwrap_or_default(),
        };

        let mut orgs = self.organizations.write().await;
        orgs.insert(org_id.clone(), org.clone());

        // Initialize quota for organization
        let quota = QuotaState {
            entity_id: org_id.clone(),
            entity_type: QuotaEntityType::Organization,
            limits: self.config.default_org_quotas.clone(),
            current_usage: ResourceUsage::default(),
        };
        let mut quotas = self.quotas.write().await;
        quotas.insert(org_id.clone(), quota);

        self.stats
            .organizations_created
            .fetch_add(1, Ordering::Relaxed);
        self.log_audit(AuditEvent::organization_created(&org)).await;

        info!("Created organization: {} ({})", org.name, org.id);
        Ok(org)
    }

    /// Get organization by ID
    pub async fn get_organization(&self, org_id: &str) -> Option<Organization> {
        let orgs = self.organizations.read().await;
        orgs.get(org_id).cloned()
    }

    /// List organizations
    pub async fn list_organizations(
        &self,
        filter: Option<OrganizationFilter>,
    ) -> Vec<Organization> {
        let orgs = self.organizations.read().await;
        let mut result: Vec<_> = orgs.values().cloned().collect();

        if let Some(filter) = filter {
            if let Some(status) = filter.status {
                result.retain(|o| o.status == status);
            }
            if let Some(plan) = filter.plan {
                result.retain(|o| o.plan == plan);
            }
        }

        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        result
    }

    /// Update organization
    pub async fn update_organization(
        &self,
        org_id: &str,
        update: UpdateOrganizationRequest,
    ) -> Result<Organization> {
        let mut orgs = self.organizations.write().await;
        let org = orgs.get_mut(org_id).ok_or_else(|| {
            StreamlineError::Config(format!("Organization not found: {}", org_id))
        })?;

        if let Some(name) = update.name {
            org.name = name;
        }
        if let Some(settings) = update.settings {
            org.settings = settings;
        }
        if let Some(plan) = update.plan {
            org.plan = plan;
        }
        org.updated_at = Utc::now();

        let updated = org.clone();
        self.log_audit(AuditEvent::organization_updated(&updated))
            .await;

        Ok(updated)
    }

    /// Delete organization
    pub async fn delete_organization(&self, org_id: &str) -> Result<()> {
        // Check for active projects
        let projects = self.projects.read().await;
        let has_projects = projects.values().any(|p| p.org_id == org_id);
        if has_projects {
            return Err(StreamlineError::Config(
                "Cannot delete organization with active projects".to_string(),
            ));
        }
        drop(projects);

        let mut orgs = self.organizations.write().await;
        let org = orgs.remove(org_id).ok_or_else(|| {
            StreamlineError::Config(format!("Organization not found: {}", org_id))
        })?;

        self.log_audit(AuditEvent::organization_deleted(&org)).await;
        info!("Deleted organization: {}", org_id);
        Ok(())
    }

    // ==================== Project Management ====================

    /// Create a new project
    pub async fn create_project(&self, request: CreateProjectRequest) -> Result<Project> {
        // Verify organization exists
        let orgs = self.organizations.read().await;
        if !orgs.contains_key(&request.org_id) {
            return Err(StreamlineError::Config(format!(
                "Organization not found: {}",
                request.org_id
            )));
        }
        drop(orgs);

        // Check quota
        if !self
            .check_quota(&request.org_id, QuotaResource::Projects, 1)
            .await?
        {
            return Err(StreamlineError::ResourceExhausted(
                "Project quota exceeded".to_string(),
            ));
        }

        let project_id = uuid::Uuid::new_v4().to_string();

        let project = Project {
            id: project_id.clone(),
            org_id: request.org_id.clone(),
            name: request.name,
            slug: request.slug,
            status: ProjectStatus::Active,
            region: request.region,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            settings: request.settings.unwrap_or_default(),
            metadata: request.metadata.unwrap_or_default(),
        };

        let mut projects = self.projects.write().await;
        projects.insert(project_id.clone(), project.clone());

        // Update quota usage
        self.increment_usage(&request.org_id, QuotaResource::Projects, 1)
            .await;

        self.stats.projects_created.fetch_add(1, Ordering::Relaxed);
        self.log_audit(AuditEvent::project_created(&project)).await;

        info!(
            "Created project: {} in org {}",
            project.name, project.org_id
        );
        Ok(project)
    }

    /// Get project by ID
    pub async fn get_project(&self, project_id: &str) -> Option<Project> {
        let projects = self.projects.read().await;
        projects.get(project_id).cloned()
    }

    /// List projects for organization
    pub async fn list_projects(&self, org_id: &str) -> Vec<Project> {
        let projects = self.projects.read().await;
        projects
            .values()
            .filter(|p| p.org_id == org_id)
            .cloned()
            .collect()
    }

    /// Delete project
    pub async fn delete_project(&self, project_id: &str) -> Result<()> {
        // Check for active clusters
        let clusters = self.clusters.read().await;
        let has_clusters = clusters.values().any(|c| c.project_id == project_id);
        if has_clusters {
            return Err(StreamlineError::Config(
                "Cannot delete project with active clusters".to_string(),
            ));
        }
        drop(clusters);

        let mut projects = self.projects.write().await;
        let project = projects
            .remove(project_id)
            .ok_or_else(|| StreamlineError::Config(format!("Project not found: {}", project_id)))?;

        // Update quota
        self.decrement_usage(&project.org_id, QuotaResource::Projects, 1)
            .await;

        self.log_audit(AuditEvent::project_deleted(&project)).await;
        info!("Deleted project: {}", project_id);
        Ok(())
    }

    // ==================== Cluster Management ====================

    /// Provision a new cluster
    pub async fn provision_cluster(
        &self,
        request: ProvisionClusterRequest,
    ) -> Result<ManagedCluster> {
        // Verify project exists
        let projects = self.projects.read().await;
        let project = projects.get(&request.project_id).ok_or_else(|| {
            StreamlineError::Config(format!("Project not found: {}", request.project_id))
        })?;
        let org_id = project.org_id.clone();
        drop(projects);

        // Check quota
        if !self
            .check_quota(&org_id, QuotaResource::Clusters, 1)
            .await?
        {
            return Err(StreamlineError::ResourceExhausted(
                "Cluster quota exceeded".to_string(),
            ));
        }

        let cluster_id = uuid::Uuid::new_v4().to_string();

        let cluster = ManagedCluster {
            id: cluster_id.clone(),
            project_id: request.project_id.clone(),
            name: request.name,
            tier: request.tier,
            region: request.region,
            status: ClusterStatus::Provisioning,
            endpoint: None,
            version: self.config.default_version.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            config: request.config.unwrap_or_default(),
            resources: ClusterResources::for_tier(request.tier),
            metadata: request.metadata.unwrap_or_default(),
        };

        let mut clusters = self.clusters.write().await;
        clusters.insert(cluster_id.clone(), cluster.clone());

        // Update quota
        self.increment_usage(&org_id, QuotaResource::Clusters, 1)
            .await;

        self.stats
            .clusters_provisioned
            .fetch_add(1, Ordering::Relaxed);
        self.log_audit(AuditEvent::cluster_provisioned(&cluster))
            .await;

        info!(
            "Provisioning cluster: {} in project {}",
            cluster.name, cluster.project_id
        );
        Ok(cluster)
    }

    /// Get cluster by ID
    pub async fn get_cluster(&self, cluster_id: &str) -> Option<ManagedCluster> {
        let clusters = self.clusters.read().await;
        clusters.get(cluster_id).cloned()
    }

    /// List clusters for project
    pub async fn list_clusters(&self, project_id: &str) -> Vec<ManagedCluster> {
        let clusters = self.clusters.read().await;
        clusters
            .values()
            .filter(|c| c.project_id == project_id)
            .cloned()
            .collect()
    }

    /// Update cluster status
    pub async fn update_cluster_status(
        &self,
        cluster_id: &str,
        status: ClusterStatus,
        endpoint: Option<String>,
    ) -> Result<ManagedCluster> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.status = status;
        if let Some(ep) = endpoint {
            cluster.endpoint = Some(ep);
        }
        cluster.updated_at = Utc::now();

        let updated = cluster.clone();
        self.log_audit(AuditEvent::cluster_status_changed(&updated))
            .await;

        Ok(updated)
    }

    /// Delete cluster
    pub async fn delete_cluster(&self, cluster_id: &str) -> Result<()> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters
            .remove(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        // Get org_id from project
        let projects = self.projects.read().await;
        if let Some(project) = projects.get(&cluster.project_id) {
            self.decrement_usage(&project.org_id, QuotaResource::Clusters, 1)
                .await;
        }

        self.log_audit(AuditEvent::cluster_deleted(&cluster)).await;
        info!("Deleted cluster: {}", cluster_id);
        Ok(())
    }

    /// Scale cluster
    pub async fn scale_cluster(
        &self,
        cluster_id: &str,
        new_tier: ClusterTier,
    ) -> Result<ManagedCluster> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.tier = new_tier;
        cluster.resources = ClusterResources::for_tier(new_tier);
        cluster.status = ClusterStatus::Scaling;
        cluster.updated_at = Utc::now();

        let updated = cluster.clone();
        self.log_audit(AuditEvent::cluster_scaled(&updated, new_tier))
            .await;

        info!("Scaling cluster {} to tier {:?}", cluster_id, new_tier);
        Ok(updated)
    }

    // ==================== Quota Management ====================

    /// Check if quota allows operation
    async fn check_quota(
        &self,
        entity_id: &str,
        resource: QuotaResource,
        amount: u64,
    ) -> Result<bool> {
        let quotas = self.quotas.read().await;
        if let Some(quota) = quotas.get(entity_id) {
            let current = quota.current_usage.get(&resource);
            let limit = quota.limits.get(&resource).unwrap_or(u64::MAX);
            Ok(current + amount <= limit)
        } else {
            Ok(true) // No quota means unlimited
        }
    }

    /// Increment usage
    async fn increment_usage(&self, entity_id: &str, resource: QuotaResource, amount: u64) {
        let mut quotas = self.quotas.write().await;
        if let Some(quota) = quotas.get_mut(entity_id) {
            quota.current_usage.increment(&resource, amount);
        }
    }

    /// Decrement usage
    async fn decrement_usage(&self, entity_id: &str, resource: QuotaResource, amount: u64) {
        let mut quotas = self.quotas.write().await;
        if let Some(quota) = quotas.get_mut(entity_id) {
            quota.current_usage.decrement(&resource, amount);
        }
    }

    /// Get quota for entity
    pub async fn get_quota(&self, entity_id: &str) -> Option<QuotaState> {
        let quotas = self.quotas.read().await;
        quotas.get(entity_id).cloned()
    }

    /// Update quota limits
    pub async fn update_quota(&self, entity_id: &str, limits: QuotaLimits) -> Result<()> {
        let mut quotas = self.quotas.write().await;
        if let Some(quota) = quotas.get_mut(entity_id) {
            quota.limits = limits;
            Ok(())
        } else {
            Err(StreamlineError::Config(format!(
                "Quota not found for: {}",
                entity_id
            )))
        }
    }

    // ==================== Audit Logging ====================

    /// Log an audit event
    async fn log_audit(&self, event: AuditEvent) {
        let mut log = self.audit_log.write().await;
        log.push(event);
        self.stats.audit_events.fetch_add(1, Ordering::Relaxed);

        // Keep last 10000 events in memory
        if log.len() > 10000 {
            log.drain(0..1000);
        }
    }

    /// Get audit events
    pub async fn get_audit_events(&self, filter: AuditFilter) -> Vec<AuditEvent> {
        let log = self.audit_log.read().await;
        let mut events: Vec<_> = log
            .iter()
            .filter(|e| {
                let mut matches = true;
                if let Some(ref org_id) = filter.org_id {
                    matches &= e.org_id.as_ref() == Some(org_id);
                }
                if let Some(ref actor_id) = filter.actor_id {
                    matches &= &e.actor_id == actor_id;
                }
                if let Some(ref action) = filter.action {
                    matches &= &e.action == action;
                }
                if let Some(since) = filter.since {
                    matches &= e.timestamp >= since;
                }
                matches
            })
            .cloned()
            .collect();

        events.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        events.truncate(filter.limit.unwrap_or(100));
        events
    }

    // ==================== Alerts ====================

    /// Create an alert
    pub async fn create_alert(&self, alert: Alert) {
        let mut alerts = self.alerts.write().await;
        alerts.push(alert);
        self.stats.alerts_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Get active alerts
    pub async fn get_active_alerts(&self, org_id: Option<&str>) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts
            .iter()
            .filter(|a| {
                a.status == AlertStatus::Active
                    && (org_id.is_none() || a.org_id.as_deref() == org_id)
            })
            .cloned()
            .collect()
    }

    /// Acknowledge alert
    pub async fn acknowledge_alert(&self, alert_id: &str, acked_by: &str) -> Result<()> {
        let mut alerts = self.alerts.write().await;
        for alert in alerts.iter_mut() {
            if alert.id == alert_id {
                alert.status = AlertStatus::Acknowledged;
                alert.acknowledged_by = Some(acked_by.to_string());
                alert.acknowledged_at = Some(Utc::now());
                return Ok(());
            }
        }
        Err(StreamlineError::Config(format!(
            "Alert not found: {}",
            alert_id
        )))
    }

    // ==================== Statistics ====================

    /// Get control plane statistics
    pub fn stats(&self) -> ControlPlaneStatsSnapshot {
        ControlPlaneStatsSnapshot {
            organizations_created: self.stats.organizations_created.load(Ordering::Relaxed),
            projects_created: self.stats.projects_created.load(Ordering::Relaxed),
            clusters_provisioned: self.stats.clusters_provisioned.load(Ordering::Relaxed),
            audit_events: self.stats.audit_events.load(Ordering::Relaxed),
            alerts_created: self.stats.alerts_created.load(Ordering::Relaxed),
        }
    }
}

// ==================== Configuration ====================

/// Control plane configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneConfig {
    /// Supported regions
    pub regions: Vec<RegionInfo>,
    /// Default Streamline version
    pub default_version: String,
    /// Default organization quotas
    pub default_org_quotas: QuotaLimits,
    /// Enable audit logging
    pub audit_enabled: bool,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            regions: vec![
                RegionInfo::new("us-east-1", "US East (N. Virginia)"),
                RegionInfo::new("us-west-2", "US West (Oregon)"),
                RegionInfo::new("eu-west-1", "EU (Ireland)"),
            ],
            default_version: "1.0.0".to_string(),
            default_org_quotas: QuotaLimits::default(),
            audit_enabled: true,
        }
    }
}

/// Region info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Region code
    pub code: String,
    /// Region name
    pub name: String,
    /// Available cluster tiers
    pub available_tiers: Vec<ClusterTier>,
}

impl RegionInfo {
    fn new(code: &str, name: &str) -> Self {
        Self {
            code: code.to_string(),
            name: name.to_string(),
            available_tiers: vec![
                ClusterTier::Developer,
                ClusterTier::Basic,
                ClusterTier::Standard,
                ClusterTier::Enterprise,
            ],
        }
    }
}

// ==================== Organization Types ====================

/// Organization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    /// Organization ID
    pub id: String,
    /// Organization name
    pub name: String,
    /// URL-safe slug
    pub slug: String,
    /// Owner user ID
    pub owner_id: String,
    /// Billing plan
    pub plan: BillingPlan,
    /// Status
    pub status: OrganizationStatus,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Settings
    pub settings: OrganizationSettings,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Organization status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrganizationStatus {
    /// Active
    Active,
    /// Suspended
    Suspended,
    /// Deleted
    Deleted,
}

/// Billing plan
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BillingPlan {
    /// Free tier
    #[default]
    Free,
    /// Developer tier
    Developer,
    /// Team tier
    Team,
    /// Business tier
    Business,
    /// Enterprise tier
    Enterprise,
}

/// Organization settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrganizationSettings {
    /// SSO enabled
    pub sso_enabled: bool,
    /// SAML provider URL
    pub saml_provider_url: Option<String>,
    /// Enforce 2FA
    pub enforce_2fa: bool,
    /// Allowed IP ranges
    pub allowed_ip_ranges: Vec<String>,
}

/// Create organization request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrganizationRequest {
    /// Organization name
    pub name: String,
    /// URL slug
    pub slug: String,
    /// Owner user ID
    pub owner_id: String,
    /// Billing plan
    pub plan: BillingPlan,
    /// Settings
    pub settings: Option<OrganizationSettings>,
    /// Metadata
    pub metadata: Option<HashMap<String, String>>,
}

/// Update organization request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateOrganizationRequest {
    /// New name
    pub name: Option<String>,
    /// New settings
    pub settings: Option<OrganizationSettings>,
    /// New plan
    pub plan: Option<BillingPlan>,
}

/// Organization filter
#[derive(Debug, Clone, Default)]
pub struct OrganizationFilter {
    /// Filter by status
    pub status: Option<OrganizationStatus>,
    /// Filter by plan
    pub plan: Option<BillingPlan>,
}

// ==================== Project Types ====================

/// Project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    /// Project ID
    pub id: String,
    /// Organization ID
    pub org_id: String,
    /// Project name
    pub name: String,
    /// URL slug
    pub slug: String,
    /// Status
    pub status: ProjectStatus,
    /// Default region
    pub region: String,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Settings
    pub settings: ProjectSettings,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Project status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectStatus {
    /// Active
    Active,
    /// Suspended
    Suspended,
    /// Deleted
    Deleted,
}

/// Project settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectSettings {
    /// Default cluster tier
    pub default_cluster_tier: Option<ClusterTier>,
    /// Enable private networking
    pub private_networking: bool,
    /// CIDR block for VPC
    pub vpc_cidr: Option<String>,
}

/// Create project request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProjectRequest {
    /// Organization ID
    pub org_id: String,
    /// Project name
    pub name: String,
    /// URL slug
    pub slug: String,
    /// Default region
    pub region: String,
    /// Settings
    pub settings: Option<ProjectSettings>,
    /// Metadata
    pub metadata: Option<HashMap<String, String>>,
}

// ==================== Cluster Types ====================

/// Managed cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedCluster {
    /// Cluster ID
    pub id: String,
    /// Project ID
    pub project_id: String,
    /// Cluster name
    pub name: String,
    /// Cluster tier
    pub tier: ClusterTier,
    /// Region
    pub region: String,
    /// Status
    pub status: ClusterStatus,
    /// Connection endpoint
    pub endpoint: Option<String>,
    /// Streamline version
    pub version: String,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Cluster configuration
    pub config: ClusterConfig,
    /// Resources
    pub resources: ClusterResources,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Cluster tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ClusterTier {
    /// Developer (free tier)
    #[default]
    Developer,
    /// Basic
    Basic,
    /// Standard
    Standard,
    /// Enterprise
    Enterprise,
    /// Dedicated
    Dedicated,
}

/// Cluster status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterStatus {
    /// Provisioning
    Provisioning,
    /// Running
    Running,
    /// Scaling
    Scaling,
    /// Upgrading
    Upgrading,
    /// Degraded
    Degraded,
    /// Stopped
    Stopped,
    /// Deleting
    Deleting,
    /// Failed
    Failed,
}

/// Cluster configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Enable TLS
    pub tls_enabled: bool,
    /// Enable authentication
    pub auth_enabled: bool,
    /// Retention hours
    pub retention_hours: u32,
    /// Maximum partitions
    pub max_partitions: u32,
    /// Enable auto-scaling
    pub auto_scaling: bool,
}

/// Cluster resources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterResources {
    /// CPU cores
    pub cpu_cores: u32,
    /// Memory in GB
    pub memory_gb: u32,
    /// Storage in GB
    pub storage_gb: u32,
    /// Network bandwidth in Gbps
    pub network_gbps: u32,
    /// Max throughput MB/s
    pub max_throughput_mbps: u32,
}

impl ClusterResources {
    /// Get resources for a cluster tier
    pub fn for_tier(tier: ClusterTier) -> Self {
        match tier {
            ClusterTier::Developer => Self {
                cpu_cores: 1,
                memory_gb: 2,
                storage_gb: 10,
                network_gbps: 1,
                max_throughput_mbps: 10,
            },
            ClusterTier::Basic => Self {
                cpu_cores: 2,
                memory_gb: 4,
                storage_gb: 50,
                network_gbps: 1,
                max_throughput_mbps: 50,
            },
            ClusterTier::Standard => Self {
                cpu_cores: 4,
                memory_gb: 16,
                storage_gb: 250,
                network_gbps: 5,
                max_throughput_mbps: 200,
            },
            ClusterTier::Enterprise => Self {
                cpu_cores: 8,
                memory_gb: 32,
                storage_gb: 1000,
                network_gbps: 10,
                max_throughput_mbps: 500,
            },
            ClusterTier::Dedicated => Self {
                cpu_cores: 16,
                memory_gb: 64,
                storage_gb: 5000,
                network_gbps: 25,
                max_throughput_mbps: 2000,
            },
        }
    }
}

impl Default for ClusterResources {
    fn default() -> Self {
        Self::for_tier(ClusterTier::Developer)
    }
}

/// Provision cluster request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionClusterRequest {
    /// Project ID
    pub project_id: String,
    /// Cluster name
    pub name: String,
    /// Cluster tier
    pub tier: ClusterTier,
    /// Region
    pub region: String,
    /// Configuration
    pub config: Option<ClusterConfig>,
    /// Metadata
    pub metadata: Option<HashMap<String, String>>,
}

// ==================== Quota Types ====================

/// Quota state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaState {
    /// Entity ID (org or project)
    pub entity_id: String,
    /// Entity type
    pub entity_type: QuotaEntityType,
    /// Quota limits
    pub limits: QuotaLimits,
    /// Current usage
    pub current_usage: ResourceUsage,
}

/// Quota entity type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaEntityType {
    /// Organization
    Organization,
    /// Project
    Project,
}

/// Quota resource types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuotaResource {
    /// Number of projects
    Projects,
    /// Number of clusters
    Clusters,
    /// Number of topics
    Topics,
    /// Storage in GB
    StorageGb,
    /// Throughput in MB/s
    ThroughputMbps,
}

/// Quota limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaLimits {
    limits: HashMap<QuotaResource, u64>,
}

impl QuotaLimits {
    /// Get limit for a resource
    pub fn get(&self, resource: &QuotaResource) -> Option<u64> {
        self.limits.get(resource).copied()
    }

    /// Set limit for a resource
    pub fn set(&mut self, resource: QuotaResource, limit: u64) {
        self.limits.insert(resource, limit);
    }
}

impl Default for QuotaLimits {
    fn default() -> Self {
        let mut limits = HashMap::new();
        limits.insert(QuotaResource::Projects, 10);
        limits.insert(QuotaResource::Clusters, 5);
        limits.insert(QuotaResource::Topics, 100);
        limits.insert(QuotaResource::StorageGb, 100);
        limits.insert(QuotaResource::ThroughputMbps, 50);
        Self { limits }
    }
}

/// Resource usage
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    usage: HashMap<QuotaResource, u64>,
}

impl ResourceUsage {
    /// Get usage for a resource
    pub fn get(&self, resource: &QuotaResource) -> u64 {
        self.usage.get(resource).copied().unwrap_or(0)
    }

    /// Increment usage
    pub fn increment(&mut self, resource: &QuotaResource, amount: u64) {
        *self.usage.entry(*resource).or_insert(0) += amount;
    }

    /// Decrement usage
    pub fn decrement(&mut self, resource: &QuotaResource, amount: u64) {
        if let Some(current) = self.usage.get_mut(resource) {
            *current = current.saturating_sub(amount);
        }
    }
}

// ==================== Audit Types ====================

/// Audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Event ID
    pub id: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Actor (user) ID
    pub actor_id: String,
    /// Organization ID
    pub org_id: Option<String>,
    /// Action performed
    pub action: String,
    /// Resource type
    pub resource_type: String,
    /// Resource ID
    pub resource_id: String,
    /// Request details
    pub request: Option<serde_json::Value>,
    /// Response/result
    pub response: Option<serde_json::Value>,
    /// IP address
    pub ip_address: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
}

impl AuditEvent {
    fn organization_created(org: &Organization) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: org.owner_id.clone(),
            org_id: Some(org.id.clone()),
            action: "organization.created".to_string(),
            resource_type: "organization".to_string(),
            resource_id: org.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn organization_updated(org: &Organization) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: org.owner_id.clone(),
            org_id: Some(org.id.clone()),
            action: "organization.updated".to_string(),
            resource_type: "organization".to_string(),
            resource_id: org.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn organization_deleted(org: &Organization) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: org.owner_id.clone(),
            org_id: Some(org.id.clone()),
            action: "organization.deleted".to_string(),
            resource_type: "organization".to_string(),
            resource_id: org.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn project_created(project: &Project) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: Some(project.org_id.clone()),
            action: "project.created".to_string(),
            resource_type: "project".to_string(),
            resource_id: project.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn project_deleted(project: &Project) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: Some(project.org_id.clone()),
            action: "project.deleted".to_string(),
            resource_type: "project".to_string(),
            resource_id: project.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn cluster_provisioned(cluster: &ManagedCluster) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: None,
            action: "cluster.provisioned".to_string(),
            resource_type: "cluster".to_string(),
            resource_id: cluster.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn cluster_status_changed(cluster: &ManagedCluster) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: None,
            action: "cluster.status_changed".to_string(),
            resource_type: "cluster".to_string(),
            resource_id: cluster.id.clone(),
            request: None,
            response: Some(serde_json::json!({"status": format!("{:?}", cluster.status)})),
            ip_address: None,
            user_agent: None,
        }
    }

    fn cluster_deleted(cluster: &ManagedCluster) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: None,
            action: "cluster.deleted".to_string(),
            resource_type: "cluster".to_string(),
            resource_id: cluster.id.clone(),
            request: None,
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }

    fn cluster_scaled(cluster: &ManagedCluster, new_tier: ClusterTier) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            actor_id: "system".to_string(),
            org_id: None,
            action: "cluster.scaled".to_string(),
            resource_type: "cluster".to_string(),
            resource_id: cluster.id.clone(),
            request: Some(serde_json::json!({"new_tier": format!("{:?}", new_tier)})),
            response: None,
            ip_address: None,
            user_agent: None,
        }
    }
}

/// Audit filter
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Filter by organization
    pub org_id: Option<String>,
    /// Filter by actor
    pub actor_id: Option<String>,
    /// Filter by action
    pub action: Option<String>,
    /// Filter since timestamp
    pub since: Option<DateTime<Utc>>,
    /// Maximum results
    pub limit: Option<usize>,
}

// ==================== Alert Types ====================

/// Alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert ID
    pub id: String,
    /// Organization ID
    pub org_id: Option<String>,
    /// Project ID
    pub project_id: Option<String>,
    /// Cluster ID
    pub cluster_id: Option<String>,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert title
    pub title: String,
    /// Alert message
    pub message: String,
    /// Alert status
    pub status: AlertStatus,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Acknowledged by
    pub acknowledged_by: Option<String>,
    /// Acknowledged at
    pub acknowledged_at: Option<DateTime<Utc>>,
    /// Resolved at
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Info
    Info,
    /// Warning
    Warning,
    /// Error
    Error,
    /// Critical
    Critical,
}

/// Alert status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertStatus {
    /// Active
    Active,
    /// Acknowledged
    Acknowledged,
    /// Resolved
    Resolved,
}

// ==================== Statistics ====================

/// Control plane stats (internal)
#[derive(Default)]
struct ControlPlaneStats {
    organizations_created: AtomicU64,
    projects_created: AtomicU64,
    clusters_provisioned: AtomicU64,
    audit_events: AtomicU64,
    alerts_created: AtomicU64,
}

/// Control plane stats snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneStatsSnapshot {
    /// Organizations created
    pub organizations_created: u64,
    /// Projects created
    pub projects_created: u64,
    /// Clusters provisioned
    pub clusters_provisioned: u64,
    /// Audit events logged
    pub audit_events: u64,
    /// Alerts created
    pub alerts_created: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_control_plane_creation() {
        let config = ControlPlaneConfig::default();
        let cp = ControlPlane::new(config);

        let orgs = cp.list_organizations(None).await;
        assert!(orgs.is_empty());
    }

    #[tokio::test]
    async fn test_organization_lifecycle() {
        let cp = ControlPlane::new(ControlPlaneConfig::default());

        // Create organization
        let request = CreateOrganizationRequest {
            name: "Test Org".to_string(),
            slug: "test-org".to_string(),
            owner_id: "user-123".to_string(),
            plan: BillingPlan::Developer,
            settings: None,
            metadata: None,
        };

        let org = cp.create_organization(request).await.unwrap();
        assert_eq!(org.name, "Test Org");
        assert_eq!(org.status, OrganizationStatus::Active);

        // Get organization
        let retrieved = cp.get_organization(&org.id).await.unwrap();
        assert_eq!(retrieved.id, org.id);

        // Update organization
        let update = UpdateOrganizationRequest {
            name: Some("Updated Org".to_string()),
            settings: None,
            plan: None,
        };
        let updated = cp.update_organization(&org.id, update).await.unwrap();
        assert_eq!(updated.name, "Updated Org");

        // Delete organization
        cp.delete_organization(&org.id).await.unwrap();
        assert!(cp.get_organization(&org.id).await.is_none());
    }

    #[tokio::test]
    async fn test_project_lifecycle() {
        let cp = ControlPlane::new(ControlPlaneConfig::default());

        // Create organization first
        let org_request = CreateOrganizationRequest {
            name: "Test Org".to_string(),
            slug: "test-org".to_string(),
            owner_id: "user-123".to_string(),
            plan: BillingPlan::Developer,
            settings: None,
            metadata: None,
        };
        let org = cp.create_organization(org_request).await.unwrap();

        // Create project
        let project_request = CreateProjectRequest {
            org_id: org.id.clone(),
            name: "Test Project".to_string(),
            slug: "test-project".to_string(),
            region: "us-east-1".to_string(),
            settings: None,
            metadata: None,
        };
        let project = cp.create_project(project_request).await.unwrap();
        assert_eq!(project.name, "Test Project");

        // List projects
        let projects = cp.list_projects(&org.id).await;
        assert_eq!(projects.len(), 1);

        // Delete project
        cp.delete_project(&project.id).await.unwrap();
        let projects = cp.list_projects(&org.id).await;
        assert!(projects.is_empty());
    }

    #[tokio::test]
    async fn test_cluster_lifecycle() {
        let cp = ControlPlane::new(ControlPlaneConfig::default());

        // Create org and project
        let org = cp
            .create_organization(CreateOrganizationRequest {
                name: "Test Org".to_string(),
                slug: "test-org".to_string(),
                owner_id: "user-123".to_string(),
                plan: BillingPlan::Developer,
                settings: None,
                metadata: None,
            })
            .await
            .unwrap();

        let project = cp
            .create_project(CreateProjectRequest {
                org_id: org.id.clone(),
                name: "Test Project".to_string(),
                slug: "test-project".to_string(),
                region: "us-east-1".to_string(),
                settings: None,
                metadata: None,
            })
            .await
            .unwrap();

        // Provision cluster
        let cluster = cp
            .provision_cluster(ProvisionClusterRequest {
                project_id: project.id.clone(),
                name: "test-cluster".to_string(),
                tier: ClusterTier::Developer,
                region: "us-east-1".to_string(),
                config: None,
                metadata: None,
            })
            .await
            .unwrap();

        assert_eq!(cluster.status, ClusterStatus::Provisioning);

        // Update status
        let updated = cp
            .update_cluster_status(
                &cluster.id,
                ClusterStatus::Running,
                Some("kafka.example.com:9092".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(updated.status, ClusterStatus::Running);

        // Scale cluster
        let scaled = cp
            .scale_cluster(&cluster.id, ClusterTier::Standard)
            .await
            .unwrap();
        assert_eq!(scaled.tier, ClusterTier::Standard);
        assert_eq!(scaled.resources.cpu_cores, 4);
    }
}
