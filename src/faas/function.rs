//! FaaS function definition and lifecycle.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Function state lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionState {
    /// Function is registered but not running.
    Inactive,
    /// Function is starting up (loading WASM module).
    Starting,
    /// Function is actively processing events.
    Active,
    /// Function is paused (triggers suspended).
    Paused,
    /// Function encountered an error.
    Error,
    /// Function is being undeployed.
    Removing,
}

/// Resource limits for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum execution time per invocation in milliseconds.
    pub max_execution_ms: u64,
    /// Maximum memory in bytes.
    pub max_memory_bytes: u64,
    /// Maximum concurrent invocations.
    pub max_concurrency: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_execution_ms: 30_000, // 30 seconds
            max_memory_bytes: 128 * 1024 * 1024, // 128 MB
            max_concurrency: 10,
        }
    }
}

/// Configuration for a FaaS function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Unique function name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// WASM module bytes (or path to .wasm file).
    pub wasm_source: WasmSource,
    /// Entry point function name within the WASM module.
    pub entry_point: String,
    /// Environment variables passed to the function.
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    /// Resource limits.
    #[serde(default)]
    pub limits: ResourceLimits,
    /// Output topic for function results (optional).
    pub output_topic: Option<String>,
    /// Dead letter topic for failed invocations.
    pub dlq_topic: Option<String>,
    /// Maximum retries on failure.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Tags for categorization.
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

fn default_max_retries() -> u32 {
    3
}

/// WASM module source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WasmSource {
    /// Inline WASM bytes (base64 encoded in JSON).
    Bytes(Vec<u8>),
    /// Path to a .wasm file on disk.
    File(String),
    /// Reference to a marketplace module.
    Marketplace { name: String, version: String },
}

/// Runtime metrics for a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetrics {
    /// Total invocations.
    pub total_invocations: u64,
    /// Successful invocations.
    pub successful_invocations: u64,
    /// Failed invocations.
    pub failed_invocations: u64,
    /// Total execution time in milliseconds.
    pub total_execution_ms: u64,
    /// Average execution time in milliseconds.
    pub avg_execution_ms: f64,
    /// Maximum execution time in milliseconds.
    pub max_execution_ms: u64,
    /// Last invocation timestamp.
    pub last_invocation: Option<DateTime<Utc>>,
    /// Current active invocations.
    pub active_invocations: u64,
}

impl Default for FunctionMetrics {
    fn default() -> Self {
        Self {
            total_invocations: 0,
            successful_invocations: 0,
            failed_invocations: 0,
            total_execution_ms: 0,
            avg_execution_ms: 0.0,
            max_execution_ms: 0,
            last_invocation: None,
            active_invocations: 0,
        }
    }
}

/// Result of a function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResult {
    /// Whether the invocation succeeded.
    pub success: bool,
    /// Output data (if any).
    pub output: Option<Vec<u8>>,
    /// Error message (if failed).
    pub error: Option<String>,
    /// Execution time in milliseconds.
    pub execution_ms: u64,
    /// Number of retries before this result.
    pub retries: u32,
}

/// A deployed FaaS function with runtime state.
pub struct FaasFunction {
    /// Function configuration.
    pub config: FunctionConfig,
    /// Current state.
    pub state: FunctionState,
    /// Deployment timestamp.
    pub deployed_at: DateTime<Utc>,
    /// Last state change.
    pub last_state_change: DateTime<Utc>,
    /// Runtime metrics (atomic for concurrent access).
    pub(crate) invocation_count: AtomicU64,
    pub(crate) success_count: AtomicU64,
    pub(crate) failure_count: AtomicU64,
    pub(crate) total_exec_ms: AtomicU64,
    pub(crate) max_exec_ms: AtomicU64,
    pub(crate) active_count: AtomicU64,
    pub(crate) last_invocation_ms: AtomicU64,
}

impl FaasFunction {
    /// Create a new function from configuration.
    pub fn new(config: FunctionConfig) -> Self {
        let now = Utc::now();
        Self {
            config,
            state: FunctionState::Inactive,
            deployed_at: now,
            last_state_change: now,
            invocation_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            total_exec_ms: AtomicU64::new(0),
            max_exec_ms: AtomicU64::new(0),
            active_count: AtomicU64::new(0),
            last_invocation_ms: AtomicU64::new(0),
        }
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> FunctionMetrics {
        let total = self.invocation_count.load(Ordering::Relaxed);
        let total_ms = self.total_exec_ms.load(Ordering::Relaxed);
        let last_ms = self.last_invocation_ms.load(Ordering::Relaxed);

        FunctionMetrics {
            total_invocations: total,
            successful_invocations: self.success_count.load(Ordering::Relaxed),
            failed_invocations: self.failure_count.load(Ordering::Relaxed),
            total_execution_ms: total_ms,
            avg_execution_ms: if total > 0 {
                total_ms as f64 / total as f64
            } else {
                0.0
            },
            max_execution_ms: self.max_exec_ms.load(Ordering::Relaxed),
            last_invocation: if last_ms > 0 {
                DateTime::from_timestamp_millis(last_ms as i64)
            } else {
                None
            },
            active_invocations: self.active_count.load(Ordering::Relaxed),
        }
    }

    /// Record an invocation result in metrics.
    pub fn record_invocation(&self, result: &InvocationResult) {
        self.invocation_count.fetch_add(1, Ordering::Relaxed);
        self.total_exec_ms
            .fetch_add(result.execution_ms, Ordering::Relaxed);
        self.last_invocation_ms
            .store(Utc::now().timestamp_millis() as u64, Ordering::Relaxed);

        if result.success {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failure_count.fetch_add(1, Ordering::Relaxed);
        }

        // Update max execution time (CAS loop)
        let mut current_max = self.max_exec_ms.load(Ordering::Relaxed);
        while result.execution_ms > current_max {
            match self.max_exec_ms.compare_exchange_weak(
                current_max,
                result.execution_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Transition to a new state.
    pub fn set_state(&mut self, new_state: FunctionState) {
        self.state = new_state;
        self.last_state_change = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FunctionConfig {
        FunctionConfig {
            name: "test-fn".to_string(),
            description: "Test function".to_string(),
            wasm_source: WasmSource::Bytes(vec![0, 97, 115, 109]), // WASM magic
            entry_point: "process".to_string(),
            env_vars: HashMap::new(),
            limits: ResourceLimits::default(),
            output_topic: Some("output".to_string()),
            dlq_topic: Some("dlq".to_string()),
            max_retries: 3,
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_function_creation() {
        let func = FaasFunction::new(test_config());
        assert_eq!(func.state, FunctionState::Inactive);
        assert_eq!(func.config.name, "test-fn");
    }

    #[test]
    fn test_metrics_recording() {
        let func = FaasFunction::new(test_config());

        func.record_invocation(&InvocationResult {
            success: true,
            output: Some(b"ok".to_vec()),
            error: None,
            execution_ms: 50,
            retries: 0,
        });

        func.record_invocation(&InvocationResult {
            success: false,
            output: None,
            error: Some("timeout".to_string()),
            execution_ms: 200,
            retries: 2,
        });

        let metrics = func.metrics();
        assert_eq!(metrics.total_invocations, 2);
        assert_eq!(metrics.successful_invocations, 1);
        assert_eq!(metrics.failed_invocations, 1);
        assert_eq!(metrics.total_execution_ms, 250);
        assert_eq!(metrics.max_execution_ms, 200);
        assert!((metrics.avg_execution_ms - 125.0).abs() < 0.01);
    }

    #[test]
    fn test_state_transition() {
        let mut func = FaasFunction::new(test_config());
        func.set_state(FunctionState::Starting);
        assert_eq!(func.state, FunctionState::Starting);
        func.set_state(FunctionState::Active);
        assert_eq!(func.state, FunctionState::Active);
    }
}
