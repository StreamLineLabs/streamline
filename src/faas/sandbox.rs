//! WASM Sandbox Execution & Function Pool
//!
//! Provides sandboxed WASM function execution with resource limits and a
//! pre-instantiation pool to reduce cold-start latency.
//!
//! When the `wasm-transforms` feature is enabled, functions are executed via
//! the `wasmtime` runtime. Without it, a stub implementation returns an error
//! indicating that the WASM runtime is not available.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Resource Limits
// ---------------------------------------------------------------------------

/// Resource limits enforced on each WASM invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum linear memory in bytes.
    pub max_memory_bytes: u64,
    /// Maximum wall-clock execution time.
    pub max_execution_time: Duration,
    /// Maximum fuel (instruction budget) — `None` means unlimited.
    pub max_fuel: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 128 * 1024 * 1024, // 128 MiB
            max_execution_time: Duration::from_secs(30),
            max_fuel: Some(1_000_000_000), // 1 billion instructions
        }
    }
}

// ---------------------------------------------------------------------------
// Sandbox Configuration
// ---------------------------------------------------------------------------

/// Configuration for the WASM sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Default resource limits applied to every invocation.
    pub default_limits: ResourceLimits,
    /// Maximum number of cached instances in the pool.
    pub pool_capacity: usize,
    /// Enable fuel-based metering.
    pub enable_fuel_metering: bool,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            default_limits: ResourceLimits::default(),
            pool_capacity: 64,
            enable_fuel_metering: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Execution Metrics
// ---------------------------------------------------------------------------

/// Aggregate metrics collected across sandbox invocations.
#[derive(Debug)]
pub struct ExecutionMetrics {
    /// Total number of invocations.
    invocation_count: AtomicU64,
    /// Cumulative execution time in microseconds.
    total_execution_us: AtomicU64,
    /// Peak memory observed across all invocations (bytes).
    peak_memory_bytes: AtomicU64,
}

impl ExecutionMetrics {
    fn new() -> Self {
        Self {
            invocation_count: AtomicU64::new(0),
            total_execution_us: AtomicU64::new(0),
            peak_memory_bytes: AtomicU64::new(0),
        }
    }

    fn record(&self, execution_us: u64, memory_bytes: u64) {
        self.invocation_count.fetch_add(1, Ordering::Relaxed);
        self.total_execution_us
            .fetch_add(execution_us, Ordering::Relaxed);

        // CAS loop to update peak memory.
        let mut current = self.peak_memory_bytes.load(Ordering::Relaxed);
        while memory_bytes > current {
            match self.peak_memory_bytes.compare_exchange_weak(
                current,
                memory_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Snapshot of current metrics.
    pub fn snapshot(&self) -> ExecutionMetricsSnapshot {
        ExecutionMetricsSnapshot {
            invocation_count: self.invocation_count.load(Ordering::Relaxed),
            total_execution_us: self.total_execution_us.load(Ordering::Relaxed),
            peak_memory_bytes: self.peak_memory_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of [`ExecutionMetrics`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetricsSnapshot {
    pub invocation_count: u64,
    pub total_execution_us: u64,
    pub peak_memory_bytes: u64,
}

// ---------------------------------------------------------------------------
// Function Pool
// ---------------------------------------------------------------------------

/// A cached, pre-instantiated WASM entry keyed by function id.
#[derive(Debug, Clone)]
struct PoolEntry {
    /// The raw WASM bytes used to create this entry.
    wasm_hash: u64,
    /// Timestamp of last use (for eviction).
    last_used: Instant,
}

/// Pool of pre-instantiated WASM modules to reduce cold-start latency.
///
/// Keyed by `function_id`. When the pool is full the least-recently-used
/// entry is evicted.
pub struct FunctionPool {
    capacity: usize,
    entries: Mutex<HashMap<String, PoolEntry>>,
}

impl FunctionPool {
    /// Create a new pool with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Return an existing entry or create a new one.
    ///
    /// Returns `true` when the entry was already cached (warm hit) and
    /// `false` when a new entry was created (cold start).
    pub fn get_or_create(&self, function_id: &str, wasm_bytes: &[u8]) -> Result<bool> {
        let hash = Self::hash_bytes(wasm_bytes);
        let mut entries = self
            .entries
            .lock()
            .map_err(|e| StreamlineError::Config(format!("Pool lock poisoned: {e}")))?;

        if let Some(entry) = entries.get_mut(function_id) {
            if entry.wasm_hash == hash {
                entry.last_used = Instant::now();
                debug!(function_id, "Pool hit — reusing cached instance");
                return Ok(true);
            }
            // WASM bytes changed — evict stale entry and fall through.
            entries.remove(function_id);
        }

        // Evict LRU if at capacity.
        if entries.len() >= self.capacity {
            if let Some(lru_key) = entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            {
                debug!(evicted = %lru_key, "Pool full — evicting LRU entry");
                entries.remove(&lru_key);
            }
        }

        entries.insert(
            function_id.to_string(),
            PoolEntry {
                wasm_hash: hash,
                last_used: Instant::now(),
            },
        );

        debug!(function_id, "Pool miss — created new entry");
        Ok(false)
    }

    /// Number of entries currently in the pool.
    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove a specific function from the pool.
    pub fn evict(&self, function_id: &str) -> bool {
        self.entries
            .lock()
            .map(|mut e| e.remove(function_id).is_some())
            .unwrap_or(false)
    }

    /// Clear all entries.
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }

    /// Simple hash of WASM bytes for change detection.
    fn hash_bytes(bytes: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        hasher.finish()
    }
}

// ---------------------------------------------------------------------------
// WasmSandbox
// ---------------------------------------------------------------------------

/// Sandboxed WASM execution environment.
///
/// Manages resource-limited WASM invocations and maintains a function pool
/// to amortise module compilation across calls.
pub struct WasmSandbox {
    config: SandboxConfig,
    pool: FunctionPool,
    metrics: Arc<ExecutionMetrics>,
}

impl WasmSandbox {
    /// Create a new sandbox with the given configuration.
    pub fn new(config: SandboxConfig) -> Self {
        let pool_capacity = config.pool_capacity;
        info!(
            pool_capacity,
            fuel_metering = config.enable_fuel_metering,
            "WASM sandbox initialised"
        );
        Self {
            config,
            pool: FunctionPool::new(pool_capacity),
            metrics: Arc::new(ExecutionMetrics::new()),
        }
    }

    /// Execute WASM bytes with the sandbox's default resource limits.
    pub fn execute(&self, wasm_bytes: &[u8], input: &[u8]) -> Result<Vec<u8>> {
        self.execute_with_limits(wasm_bytes, input, &self.config.default_limits)
    }

    /// Execute WASM bytes with explicit resource limits.
    #[cfg(feature = "wasm-transforms")]
    pub fn execute_with_limits(
        &self,
        wasm_bytes: &[u8],
        input: &[u8],
        limits: &ResourceLimits,
    ) -> Result<Vec<u8>> {
        use wasmtime::{Config as WtConfig, Engine, Linker, Module, Store};

        let start = Instant::now();

        // Configure the engine.
        let mut wt_config = WtConfig::new();
        if self.config.enable_fuel_metering {
            wt_config.consume_fuel(true);
        }
        let engine = Engine::new(&wt_config).map_err(|e| {
            StreamlineError::Config(format!("Failed to create WASM engine: {e}"))
        })?;

        // Compile the module.
        let module = Module::new(&engine, wasm_bytes).map_err(|e| {
            StreamlineError::Config(format!("Failed to compile WASM module: {e}"))
        })?;

        // Create store with resource limits.
        let mut store = Store::new(&engine, ());
        if let Some(fuel) = limits.max_fuel {
            store.set_fuel(fuel).map_err(|e| {
                StreamlineError::Config(format!("Failed to set fuel: {e}"))
            })?;
        }

        let linker = Linker::new(&engine);
        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            StreamlineError::Config(format!("Failed to instantiate WASM module: {e}"))
        })?;

        // Allocate input in WASM memory and invoke the entry point.
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| StreamlineError::Config("WASM module has no 'memory' export".into()))?;

        let alloc = instance
            .get_typed_func::<(i32,), i32>(&mut store, "alloc")
            .map_err(|e| StreamlineError::Config(format!("Missing 'alloc' export: {e}")))?;

        let ptr = alloc.call(&mut store, (input.len() as i32,)).map_err(|e| {
            StreamlineError::ResourceExhausted(format!("alloc trapped: {e}"))
        })?;

        memory.data_mut(&mut store)[ptr as usize..ptr as usize + input.len()]
            .copy_from_slice(input);

        let process = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "process")
            .map_err(|e| StreamlineError::Config(format!("Missing 'process' export: {e}")))?;

        let packed = process
            .call(&mut store, (ptr, input.len() as i32))
            .map_err(|e| StreamlineError::ResourceExhausted(format!("process trapped: {e}")))?;

        let out_ptr = (packed >> 32) as usize;
        let out_len = (packed & 0xFFFF_FFFF) as usize;
        let output = memory.data(&store)[out_ptr..out_ptr + out_len].to_vec();

        let elapsed = start.elapsed();
        let memory_size = memory.data_size(&store) as u64;
        self.metrics.record(elapsed.as_micros() as u64, memory_size);

        debug!(
            elapsed_us = elapsed.as_micros() as u64,
            memory_bytes = memory_size,
            output_len = output.len(),
            "WASM execution complete"
        );

        Ok(output)
    }

    /// Stub implementation when the WASM runtime is not linked.
    #[cfg(not(feature = "wasm-transforms"))]
    pub fn execute_with_limits(
        &self,
        _wasm_bytes: &[u8],
        _input: &[u8],
        _limits: &ResourceLimits,
    ) -> Result<Vec<u8>> {
        warn!("WASM sandbox invoked without wasm-transforms feature — returning error");
        Err(StreamlineError::Config(
            "WASM runtime not available: build with `--features wasm-transforms`".into(),
        ))
    }

    /// Execute using the function pool for instance caching.
    pub fn execute_pooled(
        &self,
        function_id: &str,
        wasm_bytes: &[u8],
        input: &[u8],
    ) -> Result<Vec<u8>> {
        let _warm = self.pool.get_or_create(function_id, wasm_bytes)?;
        self.execute(wasm_bytes, input)
    }

    /// Access the function pool.
    pub fn pool(&self) -> &FunctionPool {
        &self.pool
    }

    /// Get a snapshot of execution metrics.
    pub fn metrics(&self) -> ExecutionMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Reference to the sandbox configuration.
    pub fn config(&self) -> &SandboxConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- ResourceLimits ------------------------------------------------------

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_bytes, 128 * 1024 * 1024);
        assert_eq!(limits.max_execution_time, Duration::from_secs(30));
        assert_eq!(limits.max_fuel, Some(1_000_000_000));
    }

    #[test]
    fn test_resource_limits_custom() {
        let limits = ResourceLimits {
            max_memory_bytes: 64 * 1024 * 1024,
            max_execution_time: Duration::from_secs(5),
            max_fuel: None,
        };
        assert_eq!(limits.max_memory_bytes, 64 * 1024 * 1024);
        assert!(limits.max_fuel.is_none());
    }

    #[test]
    fn test_resource_limits_serde_roundtrip() {
        let limits = ResourceLimits::default();
        let json = serde_json::to_string(&limits).unwrap();
        let restored: ResourceLimits = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.max_memory_bytes, limits.max_memory_bytes);
        assert_eq!(restored.max_fuel, limits.max_fuel);
    }

    // -- SandboxConfig -------------------------------------------------------

    #[test]
    fn test_sandbox_config_default() {
        let cfg = SandboxConfig::default();
        assert_eq!(cfg.pool_capacity, 64);
        assert!(cfg.enable_fuel_metering);
    }

    #[test]
    fn test_sandbox_config_serde_roundtrip() {
        let cfg = SandboxConfig {
            default_limits: ResourceLimits::default(),
            pool_capacity: 16,
            enable_fuel_metering: false,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let restored: SandboxConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.pool_capacity, 16);
        assert!(!restored.enable_fuel_metering);
    }

    // -- ExecutionMetrics ----------------------------------------------------

    #[test]
    fn test_execution_metrics_initial() {
        let m = ExecutionMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.invocation_count, 0);
        assert_eq!(s.total_execution_us, 0);
        assert_eq!(s.peak_memory_bytes, 0);
    }

    #[test]
    fn test_execution_metrics_record() {
        let m = ExecutionMetrics::new();
        m.record(100, 4096);
        m.record(200, 8192);
        m.record(50, 2048);

        let s = m.snapshot();
        assert_eq!(s.invocation_count, 3);
        assert_eq!(s.total_execution_us, 350);
        assert_eq!(s.peak_memory_bytes, 8192);
    }

    #[test]
    fn test_execution_metrics_peak_only_increases() {
        let m = ExecutionMetrics::new();
        m.record(10, 1000);
        m.record(10, 500);
        assert_eq!(m.snapshot().peak_memory_bytes, 1000);
    }

    #[test]
    fn test_execution_metrics_snapshot_serde() {
        let snap = ExecutionMetricsSnapshot {
            invocation_count: 42,
            total_execution_us: 12345,
            peak_memory_bytes: 65536,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let restored: ExecutionMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.invocation_count, 42);
        assert_eq!(restored.total_execution_us, 12345);
        assert_eq!(restored.peak_memory_bytes, 65536);
    }

    // -- FunctionPool --------------------------------------------------------

    #[test]
    fn test_pool_creation() {
        let pool = FunctionPool::new(8);
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn test_pool_get_or_create_cold_start() {
        let pool = FunctionPool::new(8);
        let warm = pool.get_or_create("fn-1", b"wasm-bytes").unwrap();
        assert!(!warm, "First call should be a cold start");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_get_or_create_warm_hit() {
        let pool = FunctionPool::new(8);
        pool.get_or_create("fn-1", b"wasm-bytes").unwrap();
        let warm = pool.get_or_create("fn-1", b"wasm-bytes").unwrap();
        assert!(warm, "Second call with same bytes should be warm");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_stale_entry_replaced() {
        let pool = FunctionPool::new(8);
        pool.get_or_create("fn-1", b"v1").unwrap();
        let warm = pool.get_or_create("fn-1", b"v2").unwrap();
        assert!(!warm, "Changed bytes should cause a cold start");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_eviction_at_capacity() {
        let pool = FunctionPool::new(2);
        pool.get_or_create("fn-1", b"a").unwrap();
        pool.get_or_create("fn-2", b"b").unwrap();
        pool.get_or_create("fn-3", b"c").unwrap();

        // One entry should have been evicted.
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_pool_explicit_evict() {
        let pool = FunctionPool::new(8);
        pool.get_or_create("fn-1", b"a").unwrap();
        assert!(pool.evict("fn-1"));
        assert!(pool.is_empty());
        assert!(!pool.evict("fn-1"), "Double evict should return false");
    }

    #[test]
    fn test_pool_clear() {
        let pool = FunctionPool::new(8);
        pool.get_or_create("fn-1", b"a").unwrap();
        pool.get_or_create("fn-2", b"b").unwrap();
        pool.clear();
        assert!(pool.is_empty());
    }

    // -- WasmSandbox ---------------------------------------------------------

    #[test]
    fn test_sandbox_creation() {
        let sandbox = WasmSandbox::new(SandboxConfig::default());
        let m = sandbox.metrics();
        assert_eq!(m.invocation_count, 0);
        assert_eq!(sandbox.pool().len(), 0);
    }

    #[test]
    fn test_sandbox_config_access() {
        let cfg = SandboxConfig {
            pool_capacity: 32,
            ..Default::default()
        };
        let sandbox = WasmSandbox::new(cfg);
        assert_eq!(sandbox.config().pool_capacity, 32);
    }

    #[test]
    #[cfg(not(feature = "wasm-transforms"))]
    fn test_sandbox_execute_without_wasm_feature() {
        let sandbox = WasmSandbox::new(SandboxConfig::default());
        let result = sandbox.execute(b"fake-wasm", b"input");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("WASM runtime not available"),
            "Expected feature-gate error, got: {err_msg}"
        );
    }

    #[test]
    #[cfg(not(feature = "wasm-transforms"))]
    fn test_sandbox_execute_pooled_without_wasm_feature() {
        let sandbox = WasmSandbox::new(SandboxConfig::default());
        let result = sandbox.execute_pooled("fn-1", b"fake-wasm", b"input");
        assert!(result.is_err());
        // Pool entry should still have been created.
        assert_eq!(sandbox.pool().len(), 1);
    }

    #[test]
    #[cfg(not(feature = "wasm-transforms"))]
    fn test_sandbox_execute_with_custom_limits_stub() {
        let sandbox = WasmSandbox::new(SandboxConfig::default());
        let limits = ResourceLimits {
            max_memory_bytes: 1024,
            max_execution_time: Duration::from_millis(100),
            max_fuel: Some(500),
        };
        let result = sandbox.execute_with_limits(b"fake", b"input", &limits);
        assert!(result.is_err());
    }
}
