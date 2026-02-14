//! Cluster manager for Streamline Cloud
//!
//! Manages Kubernetes clusters for tenant workloads.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Kubernetes namespace prefix
    pub namespace_prefix: String,
    /// Container image
    pub image: String,
    /// Image pull policy
    pub image_pull_policy: String,
    /// Default CPU request
    pub default_cpu_request: String,
    /// Default memory request
    pub default_memory_request: String,
    /// Default CPU limit
    pub default_cpu_limit: String,
    /// Default memory limit
    pub default_memory_limit: String,
    /// Storage class
    pub storage_class: String,
    /// Service type
    pub service_type: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            namespace_prefix: "streamline-".to_string(),
            image: "streamline/streamline:latest".to_string(),
            image_pull_policy: "IfNotPresent".to_string(),
            default_cpu_request: "500m".to_string(),
            default_memory_request: "1Gi".to_string(),
            default_cpu_limit: "2".to_string(),
            default_memory_limit: "4Gi".to_string(),
            storage_class: "standard".to_string(),
            service_type: "LoadBalancer".to_string(),
        }
    }
}

/// Cloud cluster state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ClusterState {
    /// Being created
    #[default]
    Creating,
    /// Running and healthy
    Running,
    /// Unhealthy
    Unhealthy,
    /// Being scaled
    Scaling,
    /// Being updated
    Updating,
    /// Being deleted
    Deleting,
    /// Deleted
    Deleted,
}

impl std::fmt::Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterState::Creating => write!(f, "creating"),
            ClusterState::Running => write!(f, "running"),
            ClusterState::Unhealthy => write!(f, "unhealthy"),
            ClusterState::Scaling => write!(f, "scaling"),
            ClusterState::Updating => write!(f, "updating"),
            ClusterState::Deleting => write!(f, "deleting"),
            ClusterState::Deleted => write!(f, "deleted"),
        }
    }
}

/// Cloud cluster statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterStats {
    /// Current replicas
    pub replicas: u32,
    /// Ready replicas
    pub ready_replicas: u32,
    /// CPU usage (millicores)
    pub cpu_usage_millicores: u64,
    /// Memory usage (bytes)
    pub memory_usage_bytes: u64,
    /// Storage usage (bytes)
    pub storage_usage_bytes: u64,
    /// Messages per second (produce)
    pub produce_rate: f64,
    /// Messages per second (consume)
    pub consume_rate: f64,
    /// Active connections
    pub active_connections: u64,
}

/// Cloud cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudCluster {
    /// Cluster ID
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Kubernetes namespace
    pub namespace: String,
    /// Current state
    pub state: ClusterState,
    /// Desired replicas
    pub desired_replicas: u32,
    /// CPU millicores per replica
    pub cpu_millicores: u32,
    /// Memory MB per replica
    pub memory_mb: u32,
    /// Storage GB
    pub storage_gb: u32,
    /// Bootstrap servers endpoint
    pub bootstrap_servers: String,
    /// HTTP endpoint
    pub http_endpoint: String,
    /// Region
    pub region: String,
    /// Availability zones
    pub availability_zones: Vec<String>,
    /// Statistics
    pub stats: ClusterStats,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
    /// Labels
    pub labels: HashMap<String, String>,
}

impl CloudCluster {
    /// Create a new cluster definition
    pub fn new(id: &str, tenant_id: &str, region: &str, vcpu: u32, memory_mb: u32) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let namespace = format!("streamline-{}", id);

        Self {
            id: id.to_string(),
            tenant_id: tenant_id.to_string(),
            namespace,
            state: ClusterState::Creating,
            desired_replicas: 1,
            cpu_millicores: vcpu * 1000,
            memory_mb,
            storage_gb: 50,
            bootstrap_servers: String::new(),
            http_endpoint: String::new(),
            region: region.to_string(),
            availability_zones: Vec::new(),
            stats: ClusterStats::default(),
            created_at: now,
            updated_at: now,
            labels: HashMap::new(),
        }
    }

    /// Check if cluster is healthy
    pub fn is_healthy(&self) -> bool {
        self.state == ClusterState::Running && self.stats.ready_replicas == self.desired_replicas
    }
}

/// Cluster manager
pub struct ClusterManager {
    config: ClusterConfig,
    clusters: Arc<RwLock<HashMap<String, CloudCluster>>>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(config: ClusterConfig) -> Result<Self> {
        Ok(Self {
            config,
            clusters: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create a new cluster
    pub async fn create(
        &self,
        tenant_id: &str,
        region: &str,
        vcpu: u32,
        memory_mb: u32,
    ) -> Result<CloudCluster> {
        let cluster_id = format!(
            "cl-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("unknown")
        );
        let mut cluster = CloudCluster::new(&cluster_id, tenant_id, region, vcpu, memory_mb);

        // In a real implementation, this would call the Kubernetes API
        // For now, we simulate cluster creation
        cluster.namespace = format!("{}{}", self.config.namespace_prefix, cluster_id);
        cluster.bootstrap_servers = format!("{}.streamline.cloud:9092", cluster_id);
        cluster.http_endpoint = format!("https://{}.streamline.cloud:9094", cluster_id);
        cluster.state = ClusterState::Running;
        cluster.stats.replicas = 1;
        cluster.stats.ready_replicas = 1;

        let mut clusters = self.clusters.write().await;
        clusters.insert(cluster_id.clone(), cluster.clone());

        tracing::info!(
            cluster_id = %cluster_id,
            tenant_id = %tenant_id,
            "Created cloud cluster"
        );

        Ok(cluster)
    }

    /// Get a cluster by ID
    pub async fn get(&self, cluster_id: &str) -> Option<CloudCluster> {
        let clusters = self.clusters.read().await;
        clusters.get(cluster_id).cloned()
    }

    /// List clusters for a tenant
    pub async fn list(&self, tenant_id: &str) -> Vec<CloudCluster> {
        let clusters = self.clusters.read().await;
        clusters
            .values()
            .filter(|c| c.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Scale a cluster
    pub async fn scale(&self, cluster_id: &str, vcpu: u32, memory_mb: u32) -> Result<CloudCluster> {
        let mut clusters = self.clusters.write().await;

        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.state = ClusterState::Scaling;
        cluster.cpu_millicores = vcpu * 1000;
        cluster.memory_mb = memory_mb;
        cluster.updated_at = chrono::Utc::now().timestamp_millis();

        // Simulate scaling completion
        cluster.state = ClusterState::Running;

        tracing::info!(
            cluster_id = %cluster_id,
            vcpu = %vcpu,
            memory_mb = %memory_mb,
            "Scaled cloud cluster"
        );

        Ok(cluster.clone())
    }

    /// Scale replicas
    pub async fn scale_replicas(&self, cluster_id: &str, replicas: u32) -> Result<CloudCluster> {
        let mut clusters = self.clusters.write().await;

        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.state = ClusterState::Scaling;
        cluster.desired_replicas = replicas;
        cluster.updated_at = chrono::Utc::now().timestamp_millis();

        // Simulate scaling completion
        cluster.state = ClusterState::Running;
        cluster.stats.replicas = replicas;
        cluster.stats.ready_replicas = replicas;

        Ok(cluster.clone())
    }

    /// Delete a cluster
    pub async fn delete(&self, cluster_id: &str) -> Result<()> {
        let mut clusters = self.clusters.write().await;

        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.state = ClusterState::Deleting;
        cluster.updated_at = chrono::Utc::now().timestamp_millis();

        // In a real implementation, this would delete Kubernetes resources
        cluster.state = ClusterState::Deleted;

        tracing::info!(cluster_id = %cluster_id, "Deleted cloud cluster");

        Ok(())
    }

    /// Update cluster statistics
    pub async fn update_stats(&self, cluster_id: &str, stats: ClusterStats) -> Result<()> {
        let mut clusters = self.clusters.write().await;

        let cluster = clusters
            .get_mut(cluster_id)
            .ok_or_else(|| StreamlineError::Config(format!("Cluster not found: {}", cluster_id)))?;

        cluster.stats = stats;
        cluster.updated_at = chrono::Utc::now().timestamp_millis();

        // Update health status based on stats
        if cluster.stats.ready_replicas < cluster.desired_replicas {
            cluster.state = ClusterState::Unhealthy;
        } else if cluster.state == ClusterState::Unhealthy {
            cluster.state = ClusterState::Running;
        }

        Ok(())
    }

    /// Get cluster count
    pub async fn cluster_count(&self) -> usize {
        let clusters = self.clusters.read().await;
        clusters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_cluster() {
        let manager = ClusterManager::new(ClusterConfig::default()).unwrap();

        let cluster = manager
            .create("tenant1", "us-east-1", 2, 4096)
            .await
            .unwrap();

        assert_eq!(cluster.tenant_id, "tenant1");
        assert_eq!(cluster.region, "us-east-1");
        assert_eq!(cluster.cpu_millicores, 2000);
        assert_eq!(cluster.memory_mb, 4096);
        assert_eq!(cluster.state, ClusterState::Running);
    }

    #[tokio::test]
    async fn test_scale_cluster() {
        let manager = ClusterManager::new(ClusterConfig::default()).unwrap();

        let cluster = manager
            .create("tenant1", "us-east-1", 2, 4096)
            .await
            .unwrap();
        let scaled = manager.scale(&cluster.id, 4, 8192).await.unwrap();

        assert_eq!(scaled.cpu_millicores, 4000);
        assert_eq!(scaled.memory_mb, 8192);
    }

    #[tokio::test]
    async fn test_delete_cluster() {
        let manager = ClusterManager::new(ClusterConfig::default()).unwrap();

        let cluster = manager
            .create("tenant1", "us-east-1", 2, 4096)
            .await
            .unwrap();
        manager.delete(&cluster.id).await.unwrap();

        let deleted = manager.get(&cluster.id).await.unwrap();
        assert_eq!(deleted.state, ClusterState::Deleted);
    }

    #[test]
    fn test_cluster_health() {
        let mut cluster = CloudCluster::new("test", "tenant1", "us-east-1", 2, 4096);
        cluster.state = ClusterState::Running;
        cluster.desired_replicas = 3;
        cluster.stats.ready_replicas = 3;

        assert!(cluster.is_healthy());

        cluster.stats.ready_replicas = 2;
        assert!(!cluster.is_healthy());
    }
}
