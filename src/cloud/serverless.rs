//! Serverless infrastructure for Streamline Cloud
//!
//! Provides instant, auto-scaling serverless endpoints.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Serverless configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerlessConfig {
    /// Scale to zero when idle
    pub scale_to_zero: bool,
    /// Idle timeout before scaling to zero (seconds)
    pub idle_timeout_secs: u64,
    /// Maximum concurrent requests
    pub max_concurrent_requests: u32,
    /// Request timeout (milliseconds)
    pub request_timeout_ms: u64,
    /// Cold start timeout (milliseconds)
    pub cold_start_timeout_ms: u64,
    /// Maximum instances
    pub max_instances: u32,
    /// Minimum instances (0 = scale to zero)
    pub min_instances: u32,
}

impl Default for ServerlessConfig {
    fn default() -> Self {
        Self {
            scale_to_zero: true,
            idle_timeout_secs: 300,
            max_concurrent_requests: 100,
            request_timeout_ms: 30000,
            cold_start_timeout_ms: 10000,
            max_instances: 10,
            min_instances: 0,
        }
    }
}

/// Serverless endpoint status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ServerlessEndpointStatus {
    /// Cold (no instances running)
    #[default]
    Cold,
    /// Warming up (starting instance)
    Warming,
    /// Warm (instances running)
    Warm,
    /// Hot (high traffic)
    Hot,
    /// Scaling (adjusting instances)
    Scaling,
    /// Error
    Error,
}

impl std::fmt::Display for ServerlessEndpointStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerlessEndpointStatus::Cold => write!(f, "cold"),
            ServerlessEndpointStatus::Warming => write!(f, "warming"),
            ServerlessEndpointStatus::Warm => write!(f, "warm"),
            ServerlessEndpointStatus::Hot => write!(f, "hot"),
            ServerlessEndpointStatus::Scaling => write!(f, "scaling"),
            ServerlessEndpointStatus::Error => write!(f, "error"),
        }
    }
}

/// Serverless endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerlessEndpoint {
    /// Endpoint ID
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Endpoint URL
    pub url: String,
    /// Kafka bootstrap servers
    pub bootstrap_servers: String,
    /// HTTP API URL
    pub http_url: String,
    /// WebSocket URL
    pub ws_url: String,
    /// Current status
    pub status: ServerlessEndpointStatus,
    /// Current instance count
    pub instances: u32,
    /// Active connections
    pub active_connections: u64,
    /// Requests per second
    pub requests_per_second: f64,
    /// Last activity timestamp
    pub last_activity: i64,
    /// Created timestamp
    pub created_at: i64,
    /// Configuration
    pub config: ServerlessConfig,
}

/// Serverless instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerlessInstance {
    /// Instance ID
    pub id: String,
    /// Endpoint ID
    pub endpoint_id: String,
    /// Status
    pub status: InstanceStatus,
    /// Active requests
    pub active_requests: u32,
    /// Total requests handled
    pub total_requests: u64,
    /// Started timestamp
    pub started_at: i64,
    /// Last request timestamp
    pub last_request: i64,
}

/// Instance status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceStatus {
    /// Starting
    Starting,
    /// Running
    Running,
    /// Draining
    Draining,
    /// Stopped
    Stopped,
}

/// Serverless manager
pub struct ServerlessManager {
    config: ServerlessConfig,
    endpoints: Arc<RwLock<HashMap<String, ServerlessEndpoint>>>,
    instances: Arc<RwLock<HashMap<String, Vec<ServerlessInstance>>>>,
    total_requests: AtomicU64,
    running: AtomicBool,
}

impl ServerlessManager {
    /// Create a new serverless manager
    pub fn new(config: ServerlessConfig) -> Self {
        Self {
            config,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            instances: Arc::new(RwLock::new(HashMap::new())),
            total_requests: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }
    }

    /// Create a new serverless endpoint
    pub async fn create_endpoint(&self, tenant_id: &str) -> Result<ServerlessEndpoint> {
        let endpoint_id = format!(
            "se-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("unknown")
        );
        let now = chrono::Utc::now().timestamp_millis();

        let endpoint = ServerlessEndpoint {
            id: endpoint_id.clone(),
            tenant_id: tenant_id.to_string(),
            url: format!("https://{}.serverless.streamline.cloud", endpoint_id),
            bootstrap_servers: format!("{}.serverless.streamline.cloud:9092", endpoint_id),
            http_url: format!("https://{}.serverless.streamline.cloud:9094", endpoint_id),
            ws_url: format!("wss://{}.serverless.streamline.cloud:9094/ws", endpoint_id),
            status: if self.config.scale_to_zero {
                ServerlessEndpointStatus::Cold
            } else {
                ServerlessEndpointStatus::Warm
            },
            instances: if self.config.scale_to_zero { 0 } else { 1 },
            active_connections: 0,
            requests_per_second: 0.0,
            last_activity: now,
            created_at: now,
            config: self.config.clone(),
        };

        let mut endpoints = self.endpoints.write().await;
        endpoints.insert(endpoint_id.clone(), endpoint.clone());

        // Initialize instances list
        let mut instances = self.instances.write().await;
        instances.insert(endpoint_id.clone(), Vec::new());

        // Start minimum instances if configured
        if self.config.min_instances > 0 {
            drop(instances);
            drop(endpoints);
            for _ in 0..self.config.min_instances {
                self.start_instance(&endpoint_id).await?;
            }
        }

        tracing::info!(
            endpoint_id = %endpoint_id,
            tenant_id = %tenant_id,
            "Created serverless endpoint"
        );

        Ok(endpoint)
    }

    /// Get an endpoint
    pub async fn get_endpoint(&self, endpoint_id: &str) -> Option<ServerlessEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints.get(endpoint_id).cloned()
    }

    /// List endpoints for a tenant
    pub async fn list_endpoints(&self, tenant_id: &str) -> Vec<ServerlessEndpoint> {
        let endpoints = self.endpoints.read().await;
        endpoints
            .values()
            .filter(|e| e.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Delete an endpoint
    pub async fn delete_endpoint(&self, endpoint_id: &str) -> Result<()> {
        // Stop all instances first
        let instances = {
            let instances = self.instances.read().await;
            instances.get(endpoint_id).cloned().unwrap_or_default()
        };

        for instance in instances {
            self.stop_instance(&instance.id).await?;
        }

        // Remove endpoint
        let mut endpoints = self.endpoints.write().await;
        endpoints.remove(endpoint_id);

        // Remove instances list
        let mut instances = self.instances.write().await;
        instances.remove(endpoint_id);

        tracing::info!(endpoint_id = %endpoint_id, "Deleted serverless endpoint");

        Ok(())
    }

    /// Handle incoming request (warms up endpoint if needed)
    pub async fn handle_request(&self, endpoint_id: &str) -> Result<()> {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        let mut endpoints = self.endpoints.write().await;
        let endpoint = endpoints.get_mut(endpoint_id).ok_or_else(|| {
            StreamlineError::Config(format!("Endpoint not found: {}", endpoint_id))
        })?;

        let now = chrono::Utc::now().timestamp_millis();
        endpoint.last_activity = now;
        endpoint.active_connections += 1;

        // Warm up if cold
        if endpoint.status == ServerlessEndpointStatus::Cold {
            endpoint.status = ServerlessEndpointStatus::Warming;
            let endpoint_id = endpoint_id.to_string();
            drop(endpoints);

            // Start an instance
            self.start_instance(&endpoint_id).await?;

            let mut endpoints = self.endpoints.write().await;
            if let Some(endpoint) = endpoints.get_mut(&endpoint_id) {
                endpoint.status = ServerlessEndpointStatus::Warm;
                endpoint.instances = 1;
            }
        }

        Ok(())
    }

    /// Complete a request
    pub async fn complete_request(&self, endpoint_id: &str) -> Result<()> {
        let mut endpoints = self.endpoints.write().await;
        if let Some(endpoint) = endpoints.get_mut(endpoint_id) {
            endpoint.active_connections = endpoint.active_connections.saturating_sub(1);
        }
        Ok(())
    }

    /// Start an instance
    async fn start_instance(&self, endpoint_id: &str) -> Result<ServerlessInstance> {
        let instance_id = format!(
            "si-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("unknown")
        );
        let now = chrono::Utc::now().timestamp_millis();

        let instance = ServerlessInstance {
            id: instance_id.clone(),
            endpoint_id: endpoint_id.to_string(),
            status: InstanceStatus::Running,
            active_requests: 0,
            total_requests: 0,
            started_at: now,
            last_request: now,
        };

        let mut instances = self.instances.write().await;
        instances
            .entry(endpoint_id.to_string())
            .or_default()
            .push(instance.clone());

        tracing::debug!(
            instance_id = %instance_id,
            endpoint_id = %endpoint_id,
            "Started serverless instance"
        );

        Ok(instance)
    }

    /// Stop an instance
    async fn stop_instance(&self, instance_id: &str) -> Result<()> {
        let mut all_instances = self.instances.write().await;

        for instances in all_instances.values_mut() {
            if let Some(instance) = instances.iter_mut().find(|i| i.id == instance_id) {
                instance.status = InstanceStatus::Stopped;
            }
            instances.retain(|i| i.id != instance_id || i.status != InstanceStatus::Stopped);
        }

        Ok(())
    }

    /// Scale an endpoint
    pub async fn scale(&self, endpoint_id: &str, target_instances: u32) -> Result<()> {
        let mut endpoints = self.endpoints.write().await;
        let endpoint = endpoints.get_mut(endpoint_id).ok_or_else(|| {
            StreamlineError::Config(format!("Endpoint not found: {}", endpoint_id))
        })?;

        let current = endpoint.instances;
        let target = target_instances
            .min(endpoint.config.max_instances)
            .max(endpoint.config.min_instances);

        if target == current {
            return Ok(());
        }

        endpoint.status = ServerlessEndpointStatus::Scaling;
        drop(endpoints);

        if target > current {
            // Scale up
            for _ in current..target {
                self.start_instance(endpoint_id).await?;
            }
        } else {
            // Scale down
            let instances = {
                let instances = self.instances.read().await;
                instances.get(endpoint_id).cloned().unwrap_or_default()
            };

            let to_stop = (current - target) as usize;
            for instance in instances.iter().take(to_stop) {
                self.stop_instance(&instance.id).await?;
            }
        }

        let mut endpoints = self.endpoints.write().await;
        if let Some(endpoint) = endpoints.get_mut(endpoint_id) {
            endpoint.instances = target;
            endpoint.status = if target == 0 {
                ServerlessEndpointStatus::Cold
            } else if target > 5 {
                ServerlessEndpointStatus::Hot
            } else {
                ServerlessEndpointStatus::Warm
            };
        }

        tracing::info!(
            endpoint_id = %endpoint_id,
            from = %current,
            to = %target,
            "Scaled serverless endpoint"
        );

        Ok(())
    }

    /// Check for idle endpoints and scale to zero
    pub async fn check_idle_endpoints(&self) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let endpoints = self.endpoints.read().await;

        let idle_endpoints: Vec<String> = endpoints
            .iter()
            .filter(|(_, e)| {
                e.config.scale_to_zero
                    && e.active_connections == 0
                    && e.instances > 0
                    && (now - e.last_activity) > (e.config.idle_timeout_secs as i64 * 1000)
            })
            .map(|(id, _)| id.clone())
            .collect();

        drop(endpoints);

        for endpoint_id in idle_endpoints {
            self.scale(&endpoint_id, 0).await?;
            tracing::info!(
                endpoint_id = %endpoint_id,
                "Scaled endpoint to zero due to idle timeout"
            );
        }

        Ok(())
    }

    /// Get total requests
    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Start the serverless manager
    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        tracing::info!("Serverless manager started");
        Ok(())
    }

    /// Stop the serverless manager
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        tracing::info!("Serverless manager stopped");
        Ok(())
    }
}

impl Default for ServerlessManager {
    fn default() -> Self {
        Self::new(ServerlessConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_endpoint() {
        let manager = ServerlessManager::default();
        let endpoint = manager.create_endpoint("tenant1").await.unwrap();

        assert_eq!(endpoint.tenant_id, "tenant1");
        assert_eq!(endpoint.status, ServerlessEndpointStatus::Cold);
        assert_eq!(endpoint.instances, 0);
    }

    #[tokio::test]
    async fn test_warm_on_request() {
        let manager = ServerlessManager::default();
        let endpoint = manager.create_endpoint("tenant1").await.unwrap();

        // Should be cold initially
        assert_eq!(endpoint.status, ServerlessEndpointStatus::Cold);

        // Handle request warms up
        manager.handle_request(&endpoint.id).await.unwrap();

        let endpoint = manager.get_endpoint(&endpoint.id).await.unwrap();
        assert_eq!(endpoint.status, ServerlessEndpointStatus::Warm);
        assert_eq!(endpoint.instances, 1);
    }

    #[tokio::test]
    async fn test_scale_endpoint() {
        let manager = ServerlessManager::default();
        let endpoint = manager.create_endpoint("tenant1").await.unwrap();

        manager.scale(&endpoint.id, 3).await.unwrap();

        let endpoint = manager.get_endpoint(&endpoint.id).await.unwrap();
        assert_eq!(endpoint.instances, 3);
        assert_eq!(endpoint.status, ServerlessEndpointStatus::Warm);
    }

    #[tokio::test]
    async fn test_min_instances() {
        let config = ServerlessConfig {
            min_instances: 2,
            scale_to_zero: false,
            ..Default::default()
        };
        let manager = ServerlessManager::new(config);
        let endpoint = manager.create_endpoint("tenant1").await.unwrap();

        // Should not scale below minimum
        manager.scale(&endpoint.id, 0).await.unwrap();
        let endpoint = manager.get_endpoint(&endpoint.id).await.unwrap();
        assert_eq!(endpoint.instances, 2);
    }
}
