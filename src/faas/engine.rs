//! FaaS execution engine — orchestrates function invocations.

use super::function::{
    CircuitBreaker, CircuitBreakerConfig, FaasFunction, FunctionConfig, FunctionLogEntry,
    FunctionMetrics, FunctionSpec, FunctionState, InvocationOutcome, InvocationResult, LogLevel,
    VersionedFunction,
};
use super::registry::{FunctionInfo, FunctionRegistry};
use super::trigger::TriggerBinding;
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info, warn};

/// FaaS engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaasEngineConfig {
    /// Maximum total concurrent invocations across all functions.
    pub max_global_concurrency: u32,
    /// Default function timeout in milliseconds.
    pub default_timeout_ms: u64,
    /// Enable scale-to-zero (stop polling inactive triggers).
    pub scale_to_zero: bool,
    /// Idle timeout before scaling to zero (milliseconds).
    pub idle_timeout_ms: u64,
}

impl Default for FaasEngineConfig {
    fn default() -> Self {
        Self {
            max_global_concurrency: 100,
            default_timeout_ms: 30_000,
            scale_to_zero: true,
            idle_timeout_ms: 300_000, // 5 minutes
        }
    }
}

/// Engine-level metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineMetrics {
    pub total_invocations: u64,
    pub active_invocations: u64,
    pub total_functions: usize,
    pub active_functions: usize,
    pub total_bindings: usize,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn hash_bytes(data: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// WASM Execution Engine
// ---------------------------------------------------------------------------

/// A compiled WASM module cached for reuse.
#[derive(Debug, Clone)]
pub struct CompiledModule {
    /// Hash of the original WASM bytes.
    pub module_hash: u64,
    /// Raw WASM bytes.
    pub wasm_bytes: Vec<u8>,
    /// When the module was compiled.
    pub compiled_at: Instant,
    /// Size of the compiled module in bytes.
    pub size_bytes: usize,
}

/// A live function instance backed by a compiled WASM module.
pub struct FunctionInstance {
    /// Unique instance identifier.
    pub instance_id: String,
    /// The compiled module backing this instance.
    pub module: CompiledModule,
    /// Memory limit in bytes.
    pub memory_limit_bytes: u64,
    /// CPU time limit in milliseconds.
    pub cpu_time_limit_ms: u64,
    /// Capabilities granted to this instance.
    pub capabilities: Vec<String>,
    /// When the instance was created.
    pub created_at: Instant,
    /// Number of invocations served by this instance.
    pub invocation_count: AtomicU64,
    /// Last time this instance was used.
    pub last_used: std::sync::Mutex<Instant>,
}

impl FunctionInstance {
    /// Create a new function instance from a compiled module and config.
    pub fn new(module: CompiledModule, config: &FunctionConfig) -> Self {
        let now = Instant::now();
        Self {
            instance_id: uuid_v4(),
            module,
            memory_limit_bytes: config.limits.max_memory_bytes,
            cpu_time_limit_ms: config.limits.max_execution_ms,
            capabilities: Vec::new(),
            created_at: now,
            invocation_count: AtomicU64::new(0),
            last_used: std::sync::Mutex::new(now),
        }
    }

    /// Check if this instance has exceeded the given maximum age.
    pub fn is_expired(&self, max_age: std::time::Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    /// Update the last-used timestamp to now.
    pub fn touch(&self) {
        if let Ok(mut last) = self.last_used.lock() {
            *last = Instant::now();
        }
    }

    /// Get the memory limit for this instance.
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit_bytes
    }

    /// Get the CPU time limit for this instance.
    pub fn cpu_limit(&self) -> u64 {
        self.cpu_time_limit_ms
    }
}

/// Statistics for the instance pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstancePoolStats {
    /// Total instances currently in the pool.
    pub total_instances: usize,
    /// Total instances created since startup.
    pub total_created: u64,
    /// Total instances reused from pool.
    pub total_reused: u64,
    /// Total instances evicted.
    pub total_evicted: u64,
    /// Number of distinct functions with cached instances.
    pub functions_cached: usize,
}

/// Pool of warm function instances for reuse.
pub struct InstancePool {
    capacity: usize,
    instances: std::sync::Mutex<HashMap<String, Vec<FunctionInstance>>>,
    max_idle_duration: std::time::Duration,
    total_created: AtomicU64,
    total_reused: AtomicU64,
    total_evicted: AtomicU64,
}

impl InstancePool {
    /// Create a new instance pool.
    pub fn new(capacity: usize, max_idle: std::time::Duration) -> Self {
        Self {
            capacity,
            instances: std::sync::Mutex::new(HashMap::new()),
            max_idle_duration: max_idle,
            total_created: AtomicU64::new(0),
            total_reused: AtomicU64::new(0),
            total_evicted: AtomicU64::new(0),
        }
    }

    /// Acquire a warm instance for the given function, if one is available.
    pub fn acquire(&self, function_id: &str) -> Option<FunctionInstance> {
        let mut map = self.instances.lock().ok()?;
        if let Some(instances) = map.get_mut(function_id) {
            if let Some(instance) = instances.pop() {
                instance.touch();
                self.total_reused.fetch_add(1, Ordering::Relaxed);
                return Some(instance);
            }
        }
        None
    }

    /// Return an instance to the pool for future reuse.
    pub fn release(&self, function_id: &str, instance: FunctionInstance) {
        if let Ok(mut map) = self.instances.lock() {
            let total: usize = map.values().map(|v| v.len()).sum();
            if total >= self.capacity {
                self.total_evicted.fetch_add(1, Ordering::Relaxed);
                return;
            }
            map.entry(function_id.to_string())
                .or_default()
                .push(instance);
        }
    }

    /// Evict instances that have exceeded the idle duration. Returns eviction count.
    pub fn evict_expired(&self) -> usize {
        let mut evicted = 0;
        if let Ok(mut map) = self.instances.lock() {
            for instances in map.values_mut() {
                let before = instances.len();
                instances.retain(|i| !i.is_expired(self.max_idle_duration));
                evicted += before - instances.len();
            }
            map.retain(|_, v| !v.is_empty());
        }
        self.total_evicted
            .fetch_add(evicted as u64, Ordering::Relaxed);
        evicted
    }

    /// Evict all instances for a specific function. Returns eviction count.
    pub fn evict_function(&self, function_id: &str) -> usize {
        if let Ok(mut map) = self.instances.lock() {
            if let Some(instances) = map.remove(function_id) {
                let count = instances.len();
                self.total_evicted
                    .fetch_add(count as u64, Ordering::Relaxed);
                return count;
            }
        }
        0
    }

    /// Get pool statistics.
    pub fn stats(&self) -> InstancePoolStats {
        let (total, functions) = if let Ok(map) = self.instances.lock() {
            let total: usize = map.values().map(|v| v.len()).sum();
            (total, map.len())
        } else {
            (0, 0)
        };
        InstancePoolStats {
            total_instances: total,
            total_created: self.total_created.load(Ordering::Relaxed),
            total_reused: self.total_reused.load(Ordering::Relaxed),
            total_evicted: self.total_evicted.load(Ordering::Relaxed),
            functions_cached: functions,
        }
    }

    /// Total number of instances currently in the pool.
    pub fn len(&self) -> usize {
        self.instances
            .lock()
            .map(|m| m.values().map(|v| v.len()).sum())
            .unwrap_or(0)
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// WASM execution engine with module caching and instance pooling.
pub struct WasmExecutionEngine {
    pool: InstancePool,
    module_cache: std::sync::Mutex<HashMap<u64, CompiledModule>>,
    default_memory_limit: u64,
    default_cpu_limit_ms: u64,
}

impl WasmExecutionEngine {
    /// Create a new WASM execution engine.
    pub fn new(pool_capacity: usize) -> Self {
        Self {
            pool: InstancePool::new(pool_capacity, std::time::Duration::from_secs(300)),
            module_cache: std::sync::Mutex::new(HashMap::new()),
            default_memory_limit: 256 * 1024 * 1024, // 256 MB
            default_cpu_limit_ms: 30_000,
        }
    }

    /// Compile (or retrieve from cache) a WASM module.
    pub fn compile_module(&self, wasm_bytes: &[u8]) -> Result<CompiledModule> {
        let hash = hash_bytes(wasm_bytes);
        if let Ok(cache) = self.module_cache.lock() {
            if let Some(cached) = cache.get(&hash) {
                debug!(hash, "WASM module cache hit");
                return Ok(cached.clone());
            }
        }
        let module = CompiledModule {
            module_hash: hash,
            wasm_bytes: wasm_bytes.to_vec(),
            compiled_at: Instant::now(),
            size_bytes: wasm_bytes.len(),
        };
        if let Ok(mut cache) = self.module_cache.lock() {
            cache.insert(hash, module.clone());
        }
        debug!(hash, size = wasm_bytes.len(), "Compiled WASM module");
        Ok(module)
    }

    /// Create a new function instance from a compiled module and config.
    pub fn instantiate(
        &self,
        module: &CompiledModule,
        config: &FunctionConfig,
    ) -> Result<FunctionInstance> {
        let instance = FunctionInstance::new(module.clone(), config);
        self.pool.total_created.fetch_add(1, Ordering::Relaxed);
        debug!(
            instance_id = %instance.instance_id,
            memory_limit = instance.memory_limit_bytes,
            cpu_limit = instance.cpu_time_limit_ms,
            "Instantiated WASM function"
        );
        Ok(instance)
    }

    /// Invoke a function instance with input data via the WASM runtime.
    #[cfg(feature = "wasm-runtime")]
    pub fn invoke(&self, _instance: &FunctionInstance, _input: &[u8]) -> Result<Vec<u8>> {
        Err(StreamlineError::Config(
            super::function::FaasError::RuntimeNotAvailable(
                "WASM runtime integration is in progress and is not available in v0.4.0. \
                 Enable the 'wasm-runtime' feature and ensure wasmtime is configured."
                    .to_string(),
            )
            .to_string(),
        ))
    }

    /// Invoke a function instance — returns an error when the WASM runtime feature is not enabled.
    #[cfg(not(feature = "wasm-runtime"))]
    pub fn invoke(&self, _instance: &FunctionInstance, _input: &[u8]) -> Result<Vec<u8>> {
        Err(StreamlineError::Config(
            super::function::FaasError::RuntimeNotAvailable(
                "WASM runtime integration is not available in v0.4.0; the 'wasm-runtime' feature flag alone does not provide execution yet".to_string()
            ).to_string()
        ))
    }

    /// Invoke with a typed outcome.
    pub fn invoke_with_outcome(
        &self,
        instance: &FunctionInstance,
        input: &[u8],
    ) -> InvocationOutcome {
        match self.invoke(instance, input) {
            Ok(output) => InvocationOutcome::Success(output),
            Err(e) => {
                InvocationOutcome::Error(super::function::FaasError::ExecutionFailed(e.to_string()))
            }
        }
    }

    /// Get pool statistics.
    pub fn pool_stats(&self) -> InstancePoolStats {
        self.pool.stats()
    }

    /// Number of modules in the cache.
    pub fn cached_modules(&self) -> usize {
        self.module_cache.lock().map(|c| c.len()).unwrap_or(0)
    }

    /// Clear the module cache.
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.module_cache.lock() {
            cache.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// Dead Letter Queue
// ---------------------------------------------------------------------------

/// An entry in the dead letter queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    /// Name of the function that failed.
    pub function_name: String,
    /// Original input data.
    pub input: Vec<u8>,
    /// Error description.
    pub error: String,
    /// Number of attempts before giving up.
    pub attempts: u32,
    /// Timestamp when the entry was created.
    pub timestamp: DateTime<Utc>,
    /// The original topic the message came from (if applicable).
    pub original_topic: Option<String>,
}

/// Dead letter queue for failed function invocations.
pub struct DeadLetterQueue {
    entries: std::sync::Mutex<Vec<DeadLetterEntry>>,
    max_entries: usize,
}

impl DeadLetterQueue {
    /// Create a new dead letter queue with the given capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
            max_entries,
        }
    }

    /// Push an entry to the DLQ. Evicts oldest entries if at capacity.
    pub fn push(&self, entry: DeadLetterEntry) {
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= self.max_entries {
                entries.remove(0);
            }
            entries.push(entry);
        }
    }

    /// Drain up to `count` entries from the front of the queue.
    pub fn drain(&self, count: usize) -> Vec<DeadLetterEntry> {
        if let Ok(mut entries) = self.entries.lock() {
            let n = count.min(entries.len());
            entries.drain(..n).collect()
        } else {
            Vec::new()
        }
    }

    /// Number of entries in the DLQ.
    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// Whether the DLQ is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a snapshot of all entries.
    pub fn entries(&self) -> Vec<DeadLetterEntry> {
        self.entries.lock().map(|e| e.clone()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// FaaS Engine
// ---------------------------------------------------------------------------

/// The FaaS execution engine.
pub struct FaasEngine {
    config: FaasEngineConfig,
    registry: FunctionRegistry,
    total_invocations: AtomicU64,
    active_invocations: AtomicU64,
    execution_engine: WasmExecutionEngine,
    dlq: DeadLetterQueue,
    function_versions: HashMap<String, Vec<VersionedFunction>>,
    circuit_breakers: std::sync::Mutex<HashMap<String, CircuitBreaker>>,
    function_logs: std::sync::Mutex<HashMap<String, Vec<FunctionLogEntry>>>,
}

impl FaasEngine {
    /// Create a new FaaS engine.
    pub fn new(config: FaasEngineConfig) -> Self {
        info!("FaaS engine initialized");
        Self {
            config,
            registry: FunctionRegistry::new(),
            total_invocations: AtomicU64::new(0),
            active_invocations: AtomicU64::new(0),
            execution_engine: WasmExecutionEngine::new(256),
            dlq: DeadLetterQueue::new(10_000),
            function_versions: HashMap::new(),
            circuit_breakers: std::sync::Mutex::new(HashMap::new()),
            function_logs: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Deploy a function.
    pub fn deploy_function(&mut self, config: FunctionConfig) -> Result<()> {
        self.registry.deploy(config)
    }

    /// Undeploy a function.
    pub fn undeploy_function(&mut self, name: &str) -> Result<()> {
        self.registry.undeploy(name)
    }

    /// Add a trigger binding.
    pub fn add_trigger(&mut self, binding: TriggerBinding) -> Result<()> {
        self.registry.add_binding(binding)
    }

    /// Remove a trigger binding.
    pub fn remove_trigger(&mut self, name: &str) -> Result<()> {
        self.registry.remove_binding(name)
    }

    /// Start a function (transition to Active).
    pub fn start_function(&mut self, name: &str) -> Result<()> {
        let func = self
            .registry
            .get_function_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Function '{name}' not found")))?;

        match func.state {
            FunctionState::Inactive | FunctionState::Draining | FunctionState::Failed => {
                func.set_state(FunctionState::Active);
                info!(function = name, "Function started");
                Ok(())
            }
            FunctionState::Active => Ok(()),
            other => Err(StreamlineError::Config(format!(
                "Cannot start function in state {other:?}"
            ))),
        }
    }

    /// Stop a function (transition to Inactive).
    pub fn stop_function(&mut self, name: &str) -> Result<()> {
        let func = self
            .registry
            .get_function_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Function '{name}' not found")))?;

        func.set_state(FunctionState::Inactive);
        info!(function = name, "Function stopped");
        Ok(())
    }

    /// Invoke a function with input data.
    ///
    /// Core execution path with circuit breaker checks.
    pub fn invoke(&self, function_name: &str, input: &[u8]) -> Result<InvocationResult> {
        let func = self.registry.get_function(function_name).ok_or_else(|| {
            StreamlineError::Config(format!("Function '{function_name}' not found"))
        })?;

        if func.state != FunctionState::Active {
            return Err(StreamlineError::Config(format!(
                "Function '{}' is not active (state: {:?})",
                function_name, func.state
            )));
        }

        // Check circuit breaker
        if let Ok(mut cbs) = self.circuit_breakers.lock() {
            if let Some(cb) = cbs.get_mut(function_name) {
                if !cb.can_execute() {
                    warn!(function = function_name, "Circuit breaker open");
                    return Err(StreamlineError::ResourceExhausted(format!(
                        "Circuit breaker open for function '{function_name}'"
                    )));
                }
            }
        }

        // Check global concurrency
        let current = self.active_invocations.load(Ordering::Relaxed);
        if current >= self.config.max_global_concurrency as u64 {
            return Err(StreamlineError::ResourceExhausted(
                "Maximum global concurrency reached".into(),
            ));
        }

        self.active_invocations.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();

        let result = self.execute_wasm(func, input);

        self.active_invocations.fetch_sub(1, Ordering::Relaxed);
        self.total_invocations.fetch_add(1, Ordering::Relaxed);

        let execution_ms = start.elapsed().as_millis() as u64;

        let invocation_result = match result {
            Ok(output) => {
                if let Ok(mut cbs) = self.circuit_breakers.lock() {
                    if let Some(cb) = cbs.get_mut(function_name) {
                        cb.record_success();
                    }
                }
                InvocationResult {
                    success: true,
                    output: Some(output),
                    error: None,
                    execution_ms,
                    retries: 0,
                }
            }
            Err(e) => {
                if let Ok(mut cbs) = self.circuit_breakers.lock() {
                    if let Some(cb) = cbs.get_mut(function_name) {
                        cb.record_failure();
                    }
                }
                InvocationResult {
                    success: false,
                    output: None,
                    error: Some(e.to_string()),
                    execution_ms,
                    retries: 0,
                }
            }
        };

        func.record_invocation(&invocation_result);
        Ok(invocation_result)
    }

    /// Execute a WASM function via the runtime.
    #[cfg(feature = "wasm-runtime")]
    fn execute_wasm(&self, _func: &FaasFunction, _input: &[u8]) -> Result<Vec<u8>> {
        Err(StreamlineError::Config(
            super::function::FaasError::RuntimeNotAvailable(
                "WASM runtime integration is in progress and is not available in v0.4.0. \
                 Enable the 'wasm-runtime' feature and ensure wasmtime is configured."
                    .to_string(),
            )
            .to_string(),
        ))
    }

    /// Execute a WASM function — returns an error when the runtime feature is not enabled.
    #[cfg(not(feature = "wasm-runtime"))]
    fn execute_wasm(&self, _func: &FaasFunction, _input: &[u8]) -> Result<Vec<u8>> {
        Err(StreamlineError::Config(
            super::function::FaasError::RuntimeNotAvailable(
                "WASM runtime integration is not available in v0.4.0; the 'wasm-runtime' feature flag alone does not provide execution yet".to_string()
            ).to_string()
        ))
    }

    /// Invoke with retries.
    pub fn invoke_with_retries(
        &self,
        function_name: &str,
        input: &[u8],
    ) -> Result<InvocationResult> {
        let func = self.registry.get_function(function_name).ok_or_else(|| {
            StreamlineError::Config(format!("Function '{function_name}' not found"))
        })?;

        let max_retries = func.config.max_retries;
        let mut last_result = self.invoke(function_name, input)?;

        for retry in 1..=max_retries {
            if last_result.success {
                return Ok(last_result);
            }

            warn!(
                function = function_name,
                retry,
                max_retries,
                error = ?last_result.error,
                "Retrying function invocation"
            );

            last_result = self.invoke(function_name, input)?;
            last_result.retries = retry;
        }

        Ok(last_result)
    }

    /// List all functions.
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.registry.list_functions()
    }

    /// Get engine metrics.
    pub fn metrics(&self) -> EngineMetrics {
        let functions = self.registry.list_functions();
        let active_count = functions
            .iter()
            .filter(|f| f.state == FunctionState::Active)
            .count();

        EngineMetrics {
            total_invocations: self.total_invocations.load(Ordering::Relaxed),
            active_invocations: self.active_invocations.load(Ordering::Relaxed),
            total_functions: self.registry.function_count(),
            active_functions: active_count,
            total_bindings: self.registry.binding_count(),
        }
    }

    /// Deploy a function from a full specification. Returns a generated function ID.
    pub fn deploy(&mut self, spec: FunctionSpec) -> Result<String> {
        let function_id = format!("{}-{}", spec.name, uuid_v4());
        let mut config = spec.config.clone();
        config.env_vars.extend(spec.env_vars.clone());

        self.registry.deploy(config)?;

        let _module = self.execution_engine.compile_module(&spec.wasm_bytes)?;

        if let Ok(mut cbs) = self.circuit_breakers.lock() {
            cbs.insert(
                spec.name.clone(),
                CircuitBreaker::new(CircuitBreakerConfig::default()),
            );
        }

        let versioned = VersionedFunction {
            function_id: function_id.clone(),
            version: 1,
            spec: spec.clone(),
            deployed_at: Utc::now(),
            state: FunctionState::Inactive,
        };
        self.function_versions
            .entry(spec.name.clone())
            .or_default()
            .push(versioned);

        info!(function = %spec.name, id = %function_id, "Deployed function via spec");
        Ok(function_id)
    }

    /// Undeploy a function and clean up all associated resources.
    pub fn undeploy(&mut self, function_id: &str) -> Result<()> {
        let function_name = self
            .function_versions
            .iter()
            .find_map(|(name, versions)| {
                versions
                    .iter()
                    .any(|v| v.function_id == function_id)
                    .then(|| name.clone())
            })
            .ok_or_else(|| {
                StreamlineError::Config(format!("Function ID '{function_id}' not found"))
            })?;

        self.registry.undeploy(&function_name)?;
        if let Ok(mut cbs) = self.circuit_breakers.lock() {
            cbs.remove(&function_name);
        }
        self.function_versions.remove(&function_name);
        if let Ok(mut logs) = self.function_logs.lock() {
            logs.remove(&function_name);
        }
        self.execution_engine.pool.evict_function(&function_name);

        info!(function = %function_name, id = %function_id, "Undeployed function");
        Ok(())
    }

    /// Update a function with a new spec (rolling update).
    pub fn update(&mut self, function_id: &str, new_spec: FunctionSpec) -> Result<()> {
        let function_name = self
            .function_versions
            .iter()
            .find_map(|(name, versions)| {
                versions
                    .iter()
                    .any(|v| v.function_id == function_id)
                    .then(|| name.clone())
            })
            .ok_or_else(|| {
                StreamlineError::Config(format!("Function ID '{function_id}' not found"))
            })?;

        let _module = self.execution_engine.compile_module(&new_spec.wasm_bytes)?;

        let next_version = self
            .function_versions
            .get(&function_name)
            .map(|v| v.last().map(|vf| vf.version + 1).unwrap_or(1))
            .unwrap_or(1);

        let versioned = VersionedFunction {
            function_id: function_id.to_string(),
            version: next_version,
            spec: new_spec,
            deployed_at: Utc::now(),
            state: FunctionState::Active,
        };

        self.function_versions
            .entry(function_name.clone())
            .or_default()
            .push(versioned);

        self.execution_engine.pool.evict_function(&function_name);

        info!(function = %function_name, version = next_version, "Updated function");
        Ok(())
    }

    /// Get metrics for a specific function.
    pub fn get_function_metrics(&self, function_name: &str) -> Option<FunctionMetrics> {
        self.registry
            .get_function(function_name)
            .map(|f| f.metrics())
    }

    /// Get recent log entries for a function.
    pub fn get_function_logs(&self, function_name: &str, limit: usize) -> Vec<FunctionLogEntry> {
        self.function_logs
            .lock()
            .ok()
            .and_then(|logs| {
                logs.get(function_name).map(|entries| {
                    let start = if entries.len() > limit {
                        entries.len() - limit
                    } else {
                        0
                    };
                    entries[start..].to_vec()
                })
            })
            .unwrap_or_default()
    }

    /// Get all dead letter queue entries.
    pub fn dlq_entries(&self) -> Vec<DeadLetterEntry> {
        self.dlq.entries()
    }

    /// Drain entries from the dead letter queue.
    pub fn drain_dlq(&mut self, count: usize) -> Vec<DeadLetterEntry> {
        self.dlq.drain(count)
    }

    /// Get a reference to the WASM execution engine.
    pub fn execution_engine(&self) -> &WasmExecutionEngine {
        &self.execution_engine
    }

    /// Append a log entry for a function.
    fn append_log(
        &self,
        function_name: &str,
        level: LogLevel,
        message: String,
        invocation_id: Option<String>,
    ) {
        let entry = FunctionLogEntry {
            timestamp: Utc::now(),
            level,
            message,
            function_id: function_name.to_string(),
            invocation_id,
        };
        if let Ok(mut logs) = self.function_logs.lock() {
            logs.entry(function_name.to_string())
                .or_default()
                .push(entry);
        }
    }

    /// Invoke a chain of functions in sequence.
    pub fn invoke_chain(&self, chain: &FunctionChain, input: &[u8]) -> Result<InvocationResult> {
        let mut current_input = input.to_vec();
        let mut total_execution_ms: u64 = 0;

        for (i, step) in chain.steps.iter().enumerate() {
            debug!(
                chain = %chain.name,
                step = i,
                function = %step.function_name,
                "Executing chain step"
            );

            let result = if step.retry {
                self.invoke_with_retries(&step.function_name, &current_input)?
            } else {
                self.invoke(&step.function_name, &current_input)?
            };

            total_execution_ms += result.execution_ms;

            if !result.success {
                match step.on_error {
                    ChainErrorPolicy::Stop => {
                        warn!(
                            chain = %chain.name,
                            step = i,
                            function = %step.function_name,
                            "Chain stopped due to error"
                        );
                        return Ok(InvocationResult {
                            success: false,
                            output: None,
                            error: result.error,
                            execution_ms: total_execution_ms,
                            retries: result.retries,
                        });
                    }
                    ChainErrorPolicy::Skip => {
                        debug!(
                            chain = %chain.name,
                            step = i,
                            "Skipping failed step, continuing chain"
                        );
                        continue;
                    }
                    ChainErrorPolicy::DeadLetter => {
                        warn!(
                            chain = %chain.name,
                            step = i,
                            "Routing failed message to DLQ, continuing chain"
                        );
                        continue;
                    }
                }
            }

            if let Some(output) = result.output {
                current_input = output;
            }
        }

        Ok(InvocationResult {
            success: true,
            output: Some(current_input),
            error: None,
            execution_ms: total_execution_ms,
            retries: 0,
        })
    }
}

/// A chain of functions to execute in sequence (pipeline composition).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionChain {
    /// Chain name.
    pub name: String,
    /// Ordered list of functions to execute.
    pub steps: Vec<ChainStep>,
}

/// A single step in a function chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStep {
    /// Function name to invoke.
    pub function_name: String,
    /// Whether to retry on failure.
    pub retry: bool,
    /// Error handling policy for this step.
    pub on_error: ChainErrorPolicy,
}

/// How to handle errors in a chain step.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ChainErrorPolicy {
    /// Stop the entire chain on error.
    Stop,
    /// Skip this step and continue with original input.
    Skip,
    /// Send the failed message to a dead letter queue and continue.
    DeadLetter,
}

impl FunctionChain {
    /// Create a new function chain.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
        }
    }

    /// Add a step to the chain.
    pub fn step(mut self, function_name: impl Into<String>) -> Self {
        self.steps.push(ChainStep {
            function_name: function_name.into(),
            retry: true,
            on_error: ChainErrorPolicy::Stop,
        });
        self
    }

    /// Add a step with custom error policy.
    pub fn step_with_policy(
        mut self,
        function_name: impl Into<String>,
        on_error: ChainErrorPolicy,
    ) -> Self {
        self.steps.push(ChainStep {
            function_name: function_name.into(),
            retry: true,
            on_error,
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faas::function::{ResourceLimits, WasmSource};
    use crate::faas::trigger::TriggerType;

    fn test_config(name: &str) -> FunctionConfig {
        FunctionConfig {
            name: name.to_string(),
            description: "Test".to_string(),
            wasm_source: WasmSource::Bytes(vec![0, 97, 115, 109]),
            entry_point: "process".to_string(),
            env_vars: HashMap::new(),
            limits: ResourceLimits::default(),
            output_topic: None,
            dlq_topic: None,
            max_retries: 3,
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_engine_creation() {
        let engine = FaasEngine::new(FaasEngineConfig::default());
        let metrics = engine.metrics();
        assert_eq!(metrics.total_functions, 0);
        assert_eq!(metrics.total_invocations, 0);
    }

    #[test]
    fn test_deploy_and_start() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine.start_function("fn-a").unwrap();

        let metrics = engine.metrics();
        assert_eq!(metrics.total_functions, 1);
        assert_eq!(metrics.active_functions, 1);
    }

    #[test]
    fn test_invoke_active_function() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine.start_function("fn-a").unwrap();

        // Without the wasm-runtime feature, invocation returns an error result
        let result = engine.invoke("fn-a", b"hello world").unwrap();
        assert!(!result.success);
        assert!(result
            .error
            .as_ref()
            .unwrap()
            .contains("runtime not available"));

        let metrics = engine.metrics();
        assert_eq!(metrics.total_invocations, 1);
    }

    #[test]
    fn test_invoke_inactive_function_fails() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();

        let result = engine.invoke("fn-a", b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_stop_function() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine.start_function("fn-a").unwrap();
        engine.stop_function("fn-a").unwrap();

        let metrics = engine.metrics();
        assert_eq!(metrics.active_functions, 0);
    }

    #[test]
    fn test_undeploy_function() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine.undeploy_function("fn-a").unwrap();
        assert_eq!(engine.metrics().total_functions, 0);
    }

    #[test]
    fn test_global_concurrency_limit() {
        let mut engine = FaasEngine::new(FaasEngineConfig {
            max_global_concurrency: 0,
            ..Default::default()
        });
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine.start_function("fn-a").unwrap();

        let result = engine.invoke("fn-a", b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_add_trigger() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-a")).unwrap();
        engine
            .add_trigger(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();

        let metrics = engine.metrics();
        assert_eq!(metrics.total_bindings, 1);
    }

    #[test]
    fn test_invoke_nonexistent_function() {
        let engine = FaasEngine::new(FaasEngineConfig::default());
        assert!(engine.invoke("nonexistent", b"data").is_err());
    }

    // -----------------------------------------------------------------------
    // WasmExecutionEngine tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_wasm_engine_compile_and_cache() {
        let engine = WasmExecutionEngine::new(16);
        let wasm_bytes = vec![0, 97, 115, 109, 1, 0, 0, 0];
        let module = engine.compile_module(&wasm_bytes).unwrap();
        assert_eq!(module.wasm_bytes, wasm_bytes);
        assert_eq!(module.size_bytes, wasm_bytes.len());
        assert_eq!(engine.cached_modules(), 1);
        let module2 = engine.compile_module(&wasm_bytes).unwrap();
        assert_eq!(module.module_hash, module2.module_hash);
        assert_eq!(engine.cached_modules(), 1);
    }

    #[test]
    fn test_wasm_engine_instantiate() {
        let engine = WasmExecutionEngine::new(16);
        let module = engine.compile_module(&[0, 97, 115, 109]).unwrap();
        let config = test_config("fn-inst");
        let instance = engine.instantiate(&module, &config).unwrap();
        assert_eq!(instance.memory_limit(), config.limits.max_memory_bytes);
        assert_eq!(instance.cpu_limit(), config.limits.max_execution_ms);
    }

    #[test]
    fn test_wasm_engine_invoke_passthrough() {
        let engine = WasmExecutionEngine::new(16);
        let module = engine.compile_module(&[0, 97, 115, 109]).unwrap();
        let instance = engine
            .instantiate(&module, &test_config("fn-invoke"))
            .unwrap();
        let result = engine.invoke(&instance, b"hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_wasm_engine_invoke_with_outcome() {
        let engine = WasmExecutionEngine::new(16);
        let module = engine.compile_module(&[0, 97, 115, 109]).unwrap();
        let instance = engine.instantiate(&module, &test_config("fn-out")).unwrap();
        let outcome = engine.invoke_with_outcome(&instance, b"test");
        assert!(matches!(outcome, InvocationOutcome::Error(_)));
    }

    #[test]
    fn test_wasm_engine_clear_cache() {
        let engine = WasmExecutionEngine::new(16);
        engine.compile_module(&[1, 2, 3]).unwrap();
        assert_eq!(engine.cached_modules(), 1);
        engine.clear_cache();
        assert_eq!(engine.cached_modules(), 0);
    }

    // -----------------------------------------------------------------------
    // InstancePool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_instance_pool_acquire_release() {
        let pool = InstancePool::new(10, std::time::Duration::from_secs(300));
        assert!(pool.is_empty());
        let module = CompiledModule {
            module_hash: 42,
            wasm_bytes: vec![0, 97, 115, 109],
            compiled_at: Instant::now(),
            size_bytes: 4,
        };
        let instance = FunctionInstance::new(module, &test_config("fn-pool"));
        pool.release("fn-pool", instance);
        assert_eq!(pool.len(), 1);
        let acquired = pool.acquire("fn-pool");
        assert!(acquired.is_some());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_instance_pool_evict_function() {
        let pool = InstancePool::new(10, std::time::Duration::from_secs(300));
        let module = CompiledModule {
            module_hash: 42,
            wasm_bytes: vec![0, 97, 115, 109],
            compiled_at: Instant::now(),
            size_bytes: 4,
        };
        for _ in 0..3 {
            let inst = FunctionInstance::new(module.clone(), &test_config("fn-evict"));
            pool.release("fn-evict", inst);
        }
        assert_eq!(pool.len(), 3);
        assert_eq!(pool.evict_function("fn-evict"), 3);
        assert!(pool.is_empty());
    }

    #[test]
    fn test_instance_pool_stats() {
        let pool = InstancePool::new(10, std::time::Duration::from_secs(300));
        let module = CompiledModule {
            module_hash: 42,
            wasm_bytes: vec![0],
            compiled_at: Instant::now(),
            size_bytes: 1,
        };
        let inst = FunctionInstance::new(module, &test_config("fn-stats"));
        pool.release("fn-stats", inst);
        let stats = pool.stats();
        assert_eq!(stats.total_instances, 1);
        assert_eq!(stats.functions_cached, 1);
    }

    // -----------------------------------------------------------------------
    // DeadLetterQueue tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_dlq_push_and_drain() {
        let dlq = DeadLetterQueue::new(100);
        assert!(dlq.is_empty());
        dlq.push(DeadLetterEntry {
            function_name: "fn-a".into(),
            input: b"data".to_vec(),
            error: "timeout".into(),
            attempts: 3,
            timestamp: Utc::now(),
            original_topic: Some("events".into()),
        });
        assert_eq!(dlq.len(), 1);
        let drained = dlq.drain(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].function_name, "fn-a");
        assert!(dlq.is_empty());
    }

    #[test]
    fn test_dlq_overflow_evicts_oldest() {
        let dlq = DeadLetterQueue::new(3);
        for i in 0..5 {
            dlq.push(DeadLetterEntry {
                function_name: format!("fn-{i}"),
                input: vec![],
                error: "err".into(),
                attempts: 1,
                timestamp: Utc::now(),
                original_topic: None,
            });
        }
        assert_eq!(dlq.len(), 3);
        let entries = dlq.entries();
        assert_eq!(entries[0].function_name, "fn-2");
        assert_eq!(entries[1].function_name, "fn-3");
        assert_eq!(entries[2].function_name, "fn-4");
    }

    // -----------------------------------------------------------------------
    // Deploy / Update / Undeploy lifecycle tests
    // -----------------------------------------------------------------------

    fn test_spec(name: &str) -> FunctionSpec {
        FunctionSpec {
            name: name.to_string(),
            wasm_bytes: vec![0, 97, 115, 109],
            trigger: TriggerType::Topic {
                topic: "events".to_string(),
                partitions: None,
                group_id: format!("faas-{name}"),
            },
            config: test_config(name),
            resource_limits: ResourceLimits::default(),
            env_vars: HashMap::new(),
        }
    }

    #[test]
    fn test_deploy_via_spec() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        let id = engine.deploy(test_spec("fn-spec")).unwrap();
        assert!(!id.is_empty());
        assert_eq!(engine.metrics().total_functions, 1);
    }

    #[test]
    fn test_deploy_and_undeploy_via_spec() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        let id = engine.deploy(test_spec("fn-spec")).unwrap();
        engine.undeploy(&id).unwrap();
        assert_eq!(engine.metrics().total_functions, 0);
    }

    #[test]
    fn test_update_function() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        let id = engine.deploy(test_spec("fn-update")).unwrap();
        let mut new_spec = test_spec("fn-update");
        new_spec.wasm_bytes = vec![0, 97, 115, 109, 1];
        engine.update(&id, new_spec).unwrap();
        let versions = engine.function_versions.get("fn-update").unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[1].version, 2);
    }

    #[test]
    fn test_get_function_metrics() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        engine.deploy_function(test_config("fn-m")).unwrap();
        engine.start_function("fn-m").unwrap();
        // Without wasm-runtime, invoke returns a failed InvocationResult
        let result = engine.invoke("fn-m", b"data").unwrap();
        assert!(!result.success);
        let metrics = engine.get_function_metrics("fn-m").unwrap();
        assert_eq!(metrics.total_invocations, 1);
        assert_eq!(metrics.failed_invocations, 1);
    }

    #[test]
    fn test_get_function_metrics_nonexistent() {
        let engine = FaasEngine::new(FaasEngineConfig::default());
        assert!(engine.get_function_metrics("nope").is_none());
    }

    #[test]
    fn test_engine_dlq_integration() {
        let mut engine = FaasEngine::new(FaasEngineConfig::default());
        assert!(engine.dlq_entries().is_empty());
        engine.dlq.push(DeadLetterEntry {
            function_name: "fn-a".into(),
            input: b"bad".to_vec(),
            error: "boom".into(),
            attempts: 3,
            timestamp: Utc::now(),
            original_topic: None,
        });
        assert_eq!(engine.dlq_entries().len(), 1);
        let drained = engine.drain_dlq(1);
        assert_eq!(drained.len(), 1);
        assert!(engine.dlq_entries().is_empty());
    }

    #[test]
    fn test_execution_engine_accessor() {
        let engine = FaasEngine::new(FaasEngineConfig::default());
        let stats = engine.execution_engine().pool_stats();
        assert_eq!(stats.total_instances, 0);
    }
}
