//! Auto-scaler for Streamline Cloud
//!
//! Provides automatic scaling based on throughput, latency, and resource utilization.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Auto-scaler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalerConfig {
    /// Enable auto-scaling
    pub enabled: bool,
    /// Check interval in seconds
    pub check_interval_secs: u64,
    /// Scale up threshold (CPU %)
    pub scale_up_cpu_threshold: f64,
    /// Scale down threshold (CPU %)
    pub scale_down_cpu_threshold: f64,
    /// Scale up threshold (memory %)
    pub scale_up_memory_threshold: f64,
    /// Scale down threshold (memory %)
    pub scale_down_memory_threshold: f64,
    /// Minimum replicas
    pub min_replicas: u32,
    /// Maximum replicas
    pub max_replicas: u32,
    /// Cooldown period after scaling (seconds)
    pub cooldown_secs: u64,
    /// Target messages per second per replica
    pub target_throughput_per_replica: u64,
}

impl Default for AutoScalerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 30,
            scale_up_cpu_threshold: 70.0,
            scale_down_cpu_threshold: 30.0,
            scale_up_memory_threshold: 80.0,
            scale_down_memory_threshold: 40.0,
            min_replicas: 1,
            max_replicas: 10,
            cooldown_secs: 300,
            target_throughput_per_replica: 10000,
        }
    }
}

/// Current scaling metrics for a cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingMetrics {
    /// Cluster ID
    pub cluster_id: String,
    /// Current replica count
    pub current_replicas: u32,
    /// CPU utilization (0.0 - 100.0)
    pub cpu_utilization: f64,
    /// Memory utilization (0.0 - 100.0)
    pub memory_utilization: f64,
    /// Messages per second
    pub messages_per_second: u64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// Timestamp
    pub timestamp: i64,
}

impl ScalingMetrics {
    /// Create new metrics
    pub fn new(cluster_id: &str) -> Self {
        Self {
            cluster_id: cluster_id.to_string(),
            current_replicas: 1,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            messages_per_second: 0,
            avg_latency_ms: 0.0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Scaling decision
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalingDecision {
    /// No scaling needed
    NoChange,
    /// Scale up by N replicas
    ScaleUp(u32),
    /// Scale down by N replicas
    ScaleDown(u32),
    /// In cooldown period
    Cooldown,
}

impl std::fmt::Display for ScalingDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalingDecision::NoChange => write!(f, "no_change"),
            ScalingDecision::ScaleUp(n) => write!(f, "scale_up_{}", n),
            ScalingDecision::ScaleDown(n) => write!(f, "scale_down_{}", n),
            ScalingDecision::Cooldown => write!(f, "cooldown"),
        }
    }
}

/// Scaling event history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingEvent {
    /// Cluster ID
    pub cluster_id: String,
    /// Decision made
    pub decision: ScalingDecision,
    /// Metrics at decision time
    pub metrics: ScalingMetrics,
    /// Timestamp
    pub timestamp: i64,
}

/// Auto-scaler
pub struct AutoScaler {
    config: AutoScalerConfig,
    metrics: Arc<RwLock<HashMap<String, ScalingMetrics>>>,
    last_scale_time: Arc<RwLock<HashMap<String, i64>>>,
    events: Arc<RwLock<Vec<ScalingEvent>>>,
    running: AtomicBool,
}

impl AutoScaler {
    /// Create a new auto-scaler
    pub fn new(config: AutoScalerConfig) -> Result<Self> {
        Ok(Self {
            config,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            last_scale_time: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(Vec::new())),
            running: AtomicBool::new(false),
        })
    }

    /// Update metrics for a cluster
    pub async fn update_metrics(&self, metrics: ScalingMetrics) {
        let mut all_metrics = self.metrics.write().await;
        all_metrics.insert(metrics.cluster_id.clone(), metrics);
    }

    /// Evaluate scaling decision for a cluster
    pub async fn evaluate(&self, cluster_id: &str) -> ScalingDecision {
        if !self.config.enabled {
            return ScalingDecision::NoChange;
        }

        let metrics = {
            let all_metrics = self.metrics.read().await;
            match all_metrics.get(cluster_id) {
                Some(m) => m.clone(),
                None => return ScalingDecision::NoChange,
            }
        };

        // Check cooldown
        let now = chrono::Utc::now().timestamp_millis();
        {
            let last_scale = self.last_scale_time.read().await;
            if let Some(last) = last_scale.get(cluster_id) {
                let cooldown_ms = self.config.cooldown_secs as i64 * 1000;
                if now - last < cooldown_ms {
                    return ScalingDecision::Cooldown;
                }
            }
        }

        let decision = self.calculate_decision(&metrics);

        // Record event if scaling
        if decision != ScalingDecision::NoChange && decision != ScalingDecision::Cooldown {
            let mut events = self.events.write().await;
            events.push(ScalingEvent {
                cluster_id: cluster_id.to_string(),
                decision,
                metrics: metrics.clone(),
                timestamp: now,
            });

            // Update last scale time
            let mut last_scale = self.last_scale_time.write().await;
            last_scale.insert(cluster_id.to_string(), now);
        }

        decision
    }

    /// Calculate scaling decision based on metrics
    fn calculate_decision(&self, metrics: &ScalingMetrics) -> ScalingDecision {
        // Check for scale up conditions
        let should_scale_up = metrics.cpu_utilization > self.config.scale_up_cpu_threshold
            || metrics.memory_utilization > self.config.scale_up_memory_threshold
            || (metrics.messages_per_second / metrics.current_replicas as u64)
                > self.config.target_throughput_per_replica;

        // Check for scale down conditions
        let should_scale_down = metrics.cpu_utilization < self.config.scale_down_cpu_threshold
            && metrics.memory_utilization < self.config.scale_down_memory_threshold
            && (metrics.messages_per_second / metrics.current_replicas as u64)
                < self.config.target_throughput_per_replica / 2;

        if should_scale_up && metrics.current_replicas < self.config.max_replicas {
            // Calculate how many replicas to add
            let throughput_based = (metrics.messages_per_second as f64
                / self.config.target_throughput_per_replica as f64)
                .ceil() as u32;
            let target_replicas = throughput_based.max(metrics.current_replicas + 1);
            let add = (target_replicas - metrics.current_replicas)
                .min(self.config.max_replicas - metrics.current_replicas);
            if add > 0 {
                return ScalingDecision::ScaleUp(add);
            }
        } else if should_scale_down && metrics.current_replicas > self.config.min_replicas {
            // Scale down by 1 (conservative)
            return ScalingDecision::ScaleDown(1);
        }

        ScalingDecision::NoChange
    }

    /// Get recent scaling events
    pub async fn recent_events(&self, limit: usize) -> Vec<ScalingEvent> {
        let events = self.events.read().await;
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Start the auto-scaler
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::Config(
                "Auto-scaler already running".into(),
            ));
        }
        tracing::info!("Auto-scaler started");
        Ok(())
    }

    /// Stop the auto-scaler
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        tracing::info!("Auto-scaler stopped");
        Ok(())
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get configuration
    pub fn config(&self) -> &AutoScalerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scale_up_on_high_cpu() {
        let config = AutoScalerConfig {
            scale_up_cpu_threshold: 70.0,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config).unwrap();

        let metrics = ScalingMetrics {
            cluster_id: "test-cluster".to_string(),
            current_replicas: 2,
            cpu_utilization: 85.0, // Above threshold
            memory_utilization: 50.0,
            messages_per_second: 5000,
            avg_latency_ms: 10.0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        scaler.update_metrics(metrics).await;
        let decision = scaler.evaluate("test-cluster").await;

        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[tokio::test]
    async fn test_scale_down_on_low_usage() {
        let config = AutoScalerConfig {
            scale_down_cpu_threshold: 30.0,
            scale_down_memory_threshold: 40.0,
            target_throughput_per_replica: 10000,
            min_replicas: 1,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config).unwrap();

        let metrics = ScalingMetrics {
            cluster_id: "test-cluster".to_string(),
            current_replicas: 3,
            cpu_utilization: 15.0,     // Below threshold
            memory_utilization: 20.0,  // Below threshold
            messages_per_second: 5000, // Below target
            avg_latency_ms: 5.0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        scaler.update_metrics(metrics).await;
        let decision = scaler.evaluate("test-cluster").await;

        assert!(matches!(decision, ScalingDecision::ScaleDown(_)));
    }

    #[tokio::test]
    async fn test_no_scale_below_min() {
        let config = AutoScalerConfig {
            min_replicas: 2,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config).unwrap();

        let metrics = ScalingMetrics {
            cluster_id: "test-cluster".to_string(),
            current_replicas: 2, // At minimum
            cpu_utilization: 10.0,
            memory_utilization: 10.0,
            messages_per_second: 100,
            avg_latency_ms: 1.0,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        scaler.update_metrics(metrics).await;
        let decision = scaler.evaluate("test-cluster").await;

        assert_eq!(decision, ScalingDecision::NoChange);
    }
}
