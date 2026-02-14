//! Cluster provisioning and lifecycle management for Streamline Cloud
//!
//! Manages the full lifecycle of managed clusters: provisioning,
//! scaling, upgrading, and termination.
//!
//! # Stability: Experimental

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Desired state specification for a managed cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSpec {
    /// Cluster size class (e.g. "small", "medium", "large")
    pub size: String,
    /// Target deployment region
    pub region: String,
    /// Streamline server version to deploy
    pub version: String,
    /// Feature flags to enable on the cluster
    pub features: Vec<String>,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            size: "small".to_string(),
            region: "us-east-1".to_string(),
            version: "0.2.0".to_string(),
            features: Vec::new(),
        }
    }
}

/// Current operational state of a managed cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ManagedClusterStatus {
    /// Cluster is being provisioned
    #[default]
    Provisioning,
    /// Cluster is running and healthy
    Running,
    /// Cluster is being scaled (up or down)
    Scaling,
    /// Cluster is being upgraded to a new version
    Upgrading,
    /// Cluster is being terminated
    Terminating,
}

impl std::fmt::Display for ManagedClusterStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagedClusterStatus::Provisioning => write!(f, "provisioning"),
            ManagedClusterStatus::Running => write!(f, "running"),
            ManagedClusterStatus::Scaling => write!(f, "scaling"),
            ManagedClusterStatus::Upgrading => write!(f, "upgrading"),
            ManagedClusterStatus::Terminating => write!(f, "terminating"),
        }
    }
}

/// A step in a provisioning or mutation plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisioningStep {
    /// Human-readable description of the step
    pub description: String,
    /// Whether the step has been completed
    pub completed: bool,
}

/// A plan describing the steps to reach a desired cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisioningPlan {
    /// Cluster this plan applies to
    pub cluster_id: String,
    /// Ordered list of steps
    pub steps: Vec<ProvisioningStep>,
    /// When the plan was created
    pub created_at: DateTime<Utc>,
}

/// Internal representation of a managed cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedCluster {
    /// Unique cluster identifier
    pub id: String,
    /// Owning tenant ID
    pub tenant_id: String,
    /// Desired specification
    pub spec: ClusterSpec,
    /// Current operational status
    pub status: ManagedClusterStatus,
    /// When the cluster was created
    pub created_at: DateTime<Utc>,
    /// When the cluster was last updated
    pub updated_at: DateTime<Utc>,
}

/// Manages the lifecycle of managed cloud clusters
pub struct ProvisioningManager {
    clusters: Arc<RwLock<HashMap<String, ManagedCluster>>>,
}

impl ProvisioningManager {
    /// Create a new provisioning manager
    pub fn new() -> Self {
        Self {
            clusters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Provision a new cluster for a tenant
    pub async fn provision_cluster(
        &self,
        tenant_id: &str,
        spec: ClusterSpec,
    ) -> Result<ManagedCluster> {
        let id = format!("mc-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let now = Utc::now();

        let cluster = ManagedCluster {
            id: id.clone(),
            tenant_id: tenant_id.to_string(),
            spec,
            status: ManagedClusterStatus::Provisioning,
            created_at: now,
            updated_at: now,
        };

        let mut clusters = self.clusters.write().await;
        clusters.insert(id.clone(), cluster.clone());

        // In a real implementation this would kick off an async provisioning
        // workflow. For now we simulate immediate readiness.
        if let Some(c) = clusters.get_mut(&id) {
            c.status = ManagedClusterStatus::Running;
            c.updated_at = Utc::now();
        }

        let result = clusters.get(&id).cloned().ok_or_else(|| {
            crate::error::StreamlineError::Storage(format!(
                "cluster {} not found after provisioning",
                id
            ))
        })?;

        tracing::info!(
            cluster_id = %id,
            tenant_id = %tenant_id,
            "Managed cluster provisioned"
        );

        Ok(result)
    }

    /// Scale an existing cluster to a new size
    pub async fn scale_cluster(
        &self,
        cluster_id: &str,
        new_size: &str,
    ) -> Result<ManagedCluster> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters.get_mut(cluster_id).ok_or_else(|| {
            StreamlineError::Config(format!("Cluster not found: {}", cluster_id))
        })?;

        cluster.status = ManagedClusterStatus::Scaling;
        cluster.spec.size = new_size.to_string();
        cluster.updated_at = Utc::now();

        // Simulate scaling completion
        cluster.status = ManagedClusterStatus::Running;

        tracing::info!(cluster_id = %cluster_id, new_size = %new_size, "Cluster scaled");
        Ok(cluster.clone())
    }

    /// Upgrade a cluster to a new Streamline version
    pub async fn upgrade_cluster(
        &self,
        cluster_id: &str,
        new_version: &str,
    ) -> Result<ManagedCluster> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters.get_mut(cluster_id).ok_or_else(|| {
            StreamlineError::Config(format!("Cluster not found: {}", cluster_id))
        })?;

        cluster.status = ManagedClusterStatus::Upgrading;
        cluster.spec.version = new_version.to_string();
        cluster.updated_at = Utc::now();

        // Simulate upgrade completion
        cluster.status = ManagedClusterStatus::Running;

        tracing::info!(cluster_id = %cluster_id, version = %new_version, "Cluster upgraded");
        Ok(cluster.clone())
    }

    /// Terminate a managed cluster
    pub async fn terminate_cluster(&self, cluster_id: &str) -> Result<()> {
        let mut clusters = self.clusters.write().await;
        let cluster = clusters.get_mut(cluster_id).ok_or_else(|| {
            StreamlineError::Config(format!("Cluster not found: {}", cluster_id))
        })?;

        cluster.status = ManagedClusterStatus::Terminating;
        cluster.updated_at = Utc::now();

        // In production, actual resource cleanup would happen here.
        clusters.remove(cluster_id);

        tracing::info!(cluster_id = %cluster_id, "Cluster terminated");
        Ok(())
    }

    /// Retrieve a cluster by ID
    pub async fn get_cluster(&self, cluster_id: &str) -> Option<ManagedCluster> {
        let clusters = self.clusters.read().await;
        clusters.get(cluster_id).cloned()
    }

    /// List all clusters for a tenant
    pub async fn list_clusters(&self, tenant_id: &str) -> Vec<ManagedCluster> {
        let clusters = self.clusters.read().await;
        clusters
            .values()
            .filter(|c| c.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Generate a provisioning plan describing steps to reach the desired state
    pub fn plan_provisioning(cluster_id: &str, spec: &ClusterSpec) -> ProvisioningPlan {
        let mut steps = vec![
            ProvisioningStep {
                description: format!("Allocate {} cluster in {}", spec.size, spec.region),
                completed: false,
            },
            ProvisioningStep {
                description: format!("Deploy Streamline v{}", spec.version),
                completed: false,
            },
            ProvisioningStep {
                description: "Configure networking and TLS".to_string(),
                completed: false,
            },
            ProvisioningStep {
                description: "Run health checks".to_string(),
                completed: false,
            },
        ];

        for feature in &spec.features {
            steps.push(ProvisioningStep {
                description: format!("Enable feature: {}", feature),
                completed: false,
            });
        }

        ProvisioningPlan {
            cluster_id: cluster_id.to_string(),
            steps,
            created_at: Utc::now(),
        }
    }
}

impl Default for ProvisioningManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_provision_and_get_cluster() {
        let mgr = ProvisioningManager::new();
        let spec = ClusterSpec::default();
        let cluster = mgr.provision_cluster("tenant-1", spec).await.unwrap();

        assert_eq!(cluster.tenant_id, "tenant-1");
        assert_eq!(cluster.status, ManagedClusterStatus::Running);
        assert!(cluster.id.starts_with("mc-"));

        let fetched = mgr.get_cluster(&cluster.id).await.unwrap();
        assert_eq!(fetched.id, cluster.id);
    }

    #[tokio::test]
    async fn test_scale_cluster() {
        let mgr = ProvisioningManager::new();
        let cluster = mgr
            .provision_cluster("t1", ClusterSpec::default())
            .await
            .unwrap();

        let scaled = mgr.scale_cluster(&cluster.id, "large").await.unwrap();
        assert_eq!(scaled.spec.size, "large");
        assert_eq!(scaled.status, ManagedClusterStatus::Running);
    }

    #[tokio::test]
    async fn test_upgrade_cluster() {
        let mgr = ProvisioningManager::new();
        let cluster = mgr
            .provision_cluster("t1", ClusterSpec::default())
            .await
            .unwrap();

        let upgraded = mgr.upgrade_cluster(&cluster.id, "0.3.0").await.unwrap();
        assert_eq!(upgraded.spec.version, "0.3.0");
    }

    #[tokio::test]
    async fn test_terminate_cluster() {
        let mgr = ProvisioningManager::new();
        let cluster = mgr
            .provision_cluster("t1", ClusterSpec::default())
            .await
            .unwrap();

        mgr.terminate_cluster(&cluster.id).await.unwrap();
        assert!(mgr.get_cluster(&cluster.id).await.is_none());
    }

    #[tokio::test]
    async fn test_list_clusters() {
        let mgr = ProvisioningManager::new();
        mgr.provision_cluster("t1", ClusterSpec::default()).await.unwrap();
        mgr.provision_cluster("t1", ClusterSpec::default()).await.unwrap();
        mgr.provision_cluster("t2", ClusterSpec::default()).await.unwrap();

        assert_eq!(mgr.list_clusters("t1").await.len(), 2);
        assert_eq!(mgr.list_clusters("t2").await.len(), 1);
    }

    #[test]
    fn test_plan_provisioning() {
        let spec = ClusterSpec {
            features: vec!["auth".to_string(), "metrics".to_string()],
            ..Default::default()
        };
        let plan = ProvisioningManager::plan_provisioning("mc-123", &spec);

        assert_eq!(plan.cluster_id, "mc-123");
        // 4 base steps + 2 feature steps
        assert_eq!(plan.steps.len(), 6);
        assert!(plan.steps.last().unwrap().description.contains("metrics"));
    }

    #[test]
    fn test_managed_cluster_status_display() {
        assert_eq!(ManagedClusterStatus::Provisioning.to_string(), "provisioning");
        assert_eq!(ManagedClusterStatus::Running.to_string(), "running");
        assert_eq!(ManagedClusterStatus::Terminating.to_string(), "terminating");
    }

    #[tokio::test]
    async fn test_scale_nonexistent_cluster() {
        let mgr = ProvisioningManager::new();
        assert!(mgr.scale_cluster("nonexistent", "large").await.is_err());
    }
}
