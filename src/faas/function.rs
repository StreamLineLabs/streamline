//! FaaS function definition and lifecycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Function state lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionState {
    /// Function is registered but not running.
    Inactive,
    /// Function is cold-starting (loading WASM module from scratch).
    ColdStarting,
    /// Function is warming up (pre-warming instances).
    Warming,
    /// Function is actively processing events.
    Active,
    /// Function is draining in-flight invocations before shutdown.
    Draining,
    /// Function has encountered a fatal error.
    Failed,
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
            max_execution_ms: 30_000,            // 30 seconds
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

/// Specification for deploying a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSpec {
    /// Unique function name.
    pub name: String,
    /// Raw WASM module bytes.
    pub wasm_bytes: Vec<u8>,
    /// Trigger type that activates this function.
    pub trigger: super::trigger::TriggerType,
    /// Function configuration.
    pub config: FunctionConfig,
    /// Resource limits for execution.
    pub resource_limits: ResourceLimits,
    /// Environment variables passed to the function.
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
}

/// Errors specific to the FaaS subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaasError {
    /// WASM module failed to compile.
    CompilationFailed(String),
    /// WASM instance creation failed.
    InstantiationFailed(String),
    /// Function execution failed.
    ExecutionFailed(String),
    /// Execution exceeded the time limit.
    Timeout { limit_ms: u64, actual_ms: u64 },
    /// Execution exceeded the memory limit.
    OutOfMemory {
        limit_bytes: u64,
        requested_bytes: u64,
    },
    /// A resource limit was exceeded.
    ResourceLimitExceeded(String),
    /// The requested function was not found.
    FunctionNotFound(String),
    /// The function exists but is not in an active state.
    FunctionNotActive { name: String, state: FunctionState },
    /// The input data was invalid.
    InvalidInput(String),
    /// An internal engine error occurred.
    InternalError(String),
    /// The WASM runtime is not available (feature not enabled).
    RuntimeNotAvailable(String),
}

impl std::fmt::Display for FaasError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaasError::CompilationFailed(msg) => write!(f, "compilation failed: {msg}"),
            FaasError::InstantiationFailed(msg) => write!(f, "instantiation failed: {msg}"),
            FaasError::ExecutionFailed(msg) => write!(f, "execution failed: {msg}"),
            FaasError::Timeout {
                limit_ms,
                actual_ms,
            } => {
                write!(f, "timeout: limit {limit_ms}ms, actual {actual_ms}ms")
            }
            FaasError::OutOfMemory {
                limit_bytes,
                requested_bytes,
            } => {
                write!(
                    f,
                    "out of memory: limit {limit_bytes} bytes, requested {requested_bytes} bytes"
                )
            }
            FaasError::ResourceLimitExceeded(msg) => write!(f, "resource limit exceeded: {msg}"),
            FaasError::FunctionNotFound(name) => write!(f, "function not found: {name}"),
            FaasError::FunctionNotActive { name, state } => {
                write!(f, "function '{name}' not active (state: {state:?})")
            }
            FaasError::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            FaasError::InternalError(msg) => write!(f, "internal error: {msg}"),
            FaasError::RuntimeNotAvailable(msg) => write!(f, "runtime not available: {msg}"),
        }
    }
}

impl std::error::Error for FaasError {}

/// Outcome of a single function invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvocationOutcome {
    /// Invocation completed successfully with output bytes.
    Success(Vec<u8>),
    /// Invocation failed with a FaaS-specific error.
    Error(FaasError),
    /// Invocation timed out.
    Timeout,
    /// Invocation ran out of memory.
    Oom,
}

/// Retry policy with exponential backoff and optional DLQ routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries before giving up.
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds.
    pub max_backoff_ms: u64,
    /// Multiplier applied to backoff after each retry.
    pub backoff_multiplier: f64,
    /// Topic to route messages to after all retries are exhausted.
    pub dead_letter_topic: Option<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
            dead_letter_topic: None,
        }
    }
}

impl RetryPolicy {
    /// Calculate the backoff duration for a given attempt (0-indexed).
    pub fn backoff_for_attempt(&self, attempt: u32) -> u64 {
        let backoff = self.initial_backoff_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        (backoff as u64).min(self.max_backoff_ms)
    }
}

/// Configuration for a circuit breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before the circuit opens.
    pub failure_threshold: u32,
    /// How long to wait before transitioning from Open to HalfOpen (ms).
    pub recovery_timeout_ms: u64,
    /// Maximum calls allowed in HalfOpen state before deciding.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_ms: 60_000,
            half_open_max_calls: 3,
        }
    }
}

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Normal operation — requests pass through.
    Closed,
    /// Circuit is tripped — requests are rejected.
    Open,
    /// Trial period — limited requests allowed to test recovery.
    HalfOpen,
}

/// Circuit breaker protecting a function from cascading failures.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: CircuitState,
    consecutive_failures: u32,
    last_failure_time: Option<std::time::Instant>,
    half_open_calls: u32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: CircuitState::Closed,
            consecutive_failures: 0,
            last_failure_time: None,
            half_open_calls: 0,
        }
    }

    /// Check whether the circuit allows an execution to proceed.
    pub fn can_execute(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    let elapsed = last_failure.elapsed().as_millis() as u64;
                    if elapsed >= self.config.recovery_timeout_ms {
                        self.state = CircuitState::HalfOpen;
                        self.half_open_calls = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    self.reset();
                    true
                }
            }
            CircuitState::HalfOpen => {
                if self.half_open_calls < self.config.half_open_max_calls {
                    self.half_open_calls += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful execution — resets the circuit to Closed.
    pub fn record_success(&mut self) {
        self.state = CircuitState::Closed;
        self.consecutive_failures = 0;
        self.half_open_calls = 0;
        self.last_failure_time = None;
    }

    /// Record a failed execution — may trip the circuit to Open.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_failure_time = Some(std::time::Instant::now());
        if self.consecutive_failures >= self.config.failure_threshold {
            self.state = CircuitState::Open;
        }
    }

    /// Get the current circuit state.
    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// Reset the circuit breaker to its initial closed state.
    pub fn reset(&mut self) {
        self.state = CircuitState::Closed;
        self.consecutive_failures = 0;
        self.last_failure_time = None;
        self.half_open_calls = 0;
    }
}

/// A versioned snapshot of a deployed function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedFunction {
    /// Unique function identifier.
    pub function_id: String,
    /// Monotonically increasing version number.
    pub version: u64,
    /// Full function specification at this version.
    pub spec: FunctionSpec,
    /// Timestamp when this version was deployed.
    pub deployed_at: DateTime<Utc>,
    /// State of this version.
    pub state: FunctionState,
}

/// Log level for function log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// A log entry produced during function execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionLogEntry {
    /// Timestamp of the log entry.
    pub timestamp: DateTime<Utc>,
    /// Severity level.
    pub level: LogLevel,
    /// Log message.
    pub message: String,
    /// Function that produced this log entry.
    pub function_id: String,
    /// Invocation that produced this log entry (if applicable).
    pub invocation_id: Option<String>,
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
        func.set_state(FunctionState::ColdStarting);
        assert_eq!(func.state, FunctionState::ColdStarting);
        func.set_state(FunctionState::Active);
        assert_eq!(func.state, FunctionState::Active);
    }

    #[test]
    fn test_retry_policy_backoff() {
        let policy = RetryPolicy {
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 5000,
            backoff_multiplier: 2.0,
            dead_letter_topic: None,
        };
        assert_eq!(policy.backoff_for_attempt(0), 100);
        assert_eq!(policy.backoff_for_attempt(1), 200);
        assert_eq!(policy.backoff_for_attempt(2), 400);
        assert_eq!(policy.backoff_for_attempt(3), 800);
        assert_eq!(policy.backoff_for_attempt(4), 1600);
        assert_eq!(policy.backoff_for_attempt(10), 5000);
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_backoff_ms, 100);
        assert_eq!(policy.max_backoff_ms, 30_000);
        assert!((policy.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!(policy.dead_letter_topic.is_none());
    }

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout_ms: 1000,
            half_open_max_calls: 2,
        };
        let mut cb = CircuitBreaker::new(config);
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.can_execute());
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout_ms: 1000,
            half_open_max_calls: 1,
        };
        let mut cb = CircuitBreaker::new(config);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_ms: 1000,
            half_open_max_calls: 1,
        };
        let mut cb = CircuitBreaker::new(config);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.can_execute());
    }

    #[test]
    fn test_faas_error_display() {
        let err = FaasError::CompilationFailed("bad bytecode".into());
        assert_eq!(err.to_string(), "compilation failed: bad bytecode");
        let err = FaasError::Timeout {
            limit_ms: 100,
            actual_ms: 250,
        };
        assert_eq!(err.to_string(), "timeout: limit 100ms, actual 250ms");
        let err = FaasError::OutOfMemory {
            limit_bytes: 1024,
            requested_bytes: 2048,
        };
        assert!(err.to_string().contains("out of memory"));
        let err = FaasError::FunctionNotFound("my-fn".into());
        assert_eq!(err.to_string(), "function not found: my-fn");
        let err = FaasError::FunctionNotActive {
            name: "fn-a".into(),
            state: FunctionState::Inactive,
        };
        assert!(err.to_string().contains("fn-a"));
    }

    #[test]
    fn test_invocation_outcome_variants() {
        let success = InvocationOutcome::Success(vec![1, 2, 3]);
        assert!(matches!(success, InvocationOutcome::Success(ref v) if v.len() == 3));
        let err = InvocationOutcome::Error(FaasError::ExecutionFailed("boom".into()));
        assert!(matches!(err, InvocationOutcome::Error(_)));
        let timeout = InvocationOutcome::Timeout;
        assert!(matches!(timeout, InvocationOutcome::Timeout));
        let oom = InvocationOutcome::Oom;
        assert!(matches!(oom, InvocationOutcome::Oom));
    }

    #[test]
    fn test_function_spec_serialization() {
        use crate::faas::trigger::TriggerType;
        let spec = FunctionSpec {
            name: "test-spec".to_string(),
            wasm_bytes: vec![0, 97, 115, 109],
            trigger: TriggerType::Schedule {
                cron: "0 * * * *".to_string(),
            },
            config: test_config(),
            resource_limits: ResourceLimits::default(),
            env_vars: HashMap::new(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: FunctionSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "test-spec");
        assert_eq!(deserialized.wasm_bytes, vec![0, 97, 115, 109]);
    }
}
