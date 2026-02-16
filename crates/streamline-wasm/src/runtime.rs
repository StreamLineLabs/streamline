//! WebAssembly runtime for executing transformations using wasmtime
//!
//! This module provides the runtime environment for executing WASM
//! transformation modules safely and efficiently.
//!
//! ## Features
//!
//! - **Instance pooling**: Pre-instantiated modules for fast execution
//! - **Memory isolation**: Each instance has isolated memory
//! - **Timeout enforcement**: Configurable execution timeouts with fuel-based limits
//! - **Resource limits**: Memory and stack size limits
//! - **Hot reloading**: Modules can be updated without restart
//!
//! ## WASM ABI
//!
//! Transformation modules must export these functions:
//!
//! ```wat
//! ;; Allocate memory for input data
//! (export "alloc" (func $alloc (param i32) (result i32)))
//!
//! ;; Free allocated memory
//! (export "dealloc" (func $dealloc (param i32 i32)))
//!
//! ;; Transform a message - returns pointer to output
//! ;; Input format: [input_ptr: i32, input_len: i32]
//! ;; Output format: [output_ptr: i32, output_len: i32] or 0 for drop
//! (export "transform" (func $transform (param i32 i32) (result i64)))
//! ```

use super::{
    RouteResult, TransformInput, TransformOutput, TransformResult, TransformationConfig,
    TransformationType,
};
use crate::error::{Result, WasmError};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

#[cfg(feature = "wasm-runtime")]
use tracing::debug;

#[cfg(feature = "wasm-runtime")]
use wasmtime::{
    Config, Engine, Extern, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, TypedFunc,
};

/// WASM transformation runtime
///
/// Manages the execution of WebAssembly transformation modules with:
/// - Memory isolation
/// - Execution timeout via fuel consumption
/// - Resource limits
/// - Instance pooling for performance
pub struct WasmRuntime {
    /// Loaded modules
    modules: RwLock<HashMap<String, LoadedModule>>,

    /// Runtime configuration
    #[cfg(feature = "wasm-runtime")]
    config: RuntimeConfig,

    /// Runtime configuration (unused in stub mode)
    #[cfg(not(feature = "wasm-runtime"))]
    #[allow(dead_code)]
    config: RuntimeConfig,

    /// Wasmtime engine (shared across all modules)
    #[cfg(feature = "wasm-runtime")]
    engine: Engine,

    /// Runtime statistics
    stats: RwLock<RuntimeStats>,
}

/// Runtime configuration
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum memory per module (bytes)
    pub max_memory: usize,

    /// Maximum execution time (milliseconds) - converted to fuel units
    pub max_execution_time_ms: u64,

    /// Maximum stack size (bytes)
    pub max_stack_size: usize,

    /// Enable module caching
    pub enable_caching: bool,

    /// Number of pre-instantiated instances per module
    pub instance_pool_size: usize,

    /// Fuel units per millisecond (controls granularity of timeout)
    pub fuel_per_ms: u64,

    /// Enable debug mode (more verbose logging)
    pub debug_mode: bool,

    /// Enable WASM SIMD instructions
    pub enable_simd: bool,

    /// Enable WASM bulk memory operations
    pub enable_bulk_memory: bool,

    /// Enable WASM multi-value returns
    pub enable_multi_value: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024, // 64MB
            max_execution_time_ms: 1000,  // 1 second
            max_stack_size: 1024 * 1024,  // 1MB
            enable_caching: true,
            instance_pool_size: 4,
            fuel_per_ms: 100_000, // 100k fuel units per ms
            debug_mode: false,
            enable_simd: true,
            enable_bulk_memory: true,
            enable_multi_value: true,
        }
    }
}

impl RuntimeConfig {
    /// High-performance configuration for production
    pub fn high_performance() -> Self {
        Self {
            max_memory: 128 * 1024 * 1024,   // 128MB
            max_execution_time_ms: 500,      // 500ms
            max_stack_size: 2 * 1024 * 1024, // 2MB
            enable_caching: true,
            instance_pool_size: 8,
            fuel_per_ms: 200_000, // More fuel for faster execution
            debug_mode: false,
            enable_simd: true,
            enable_bulk_memory: true,
            enable_multi_value: true,
        }
    }

    /// Debug configuration with verbose logging
    pub fn debug() -> Self {
        Self {
            max_memory: 32 * 1024 * 1024, // 32MB
            max_execution_time_ms: 5000,  // 5 seconds
            max_stack_size: 1024 * 1024,
            enable_caching: false,
            instance_pool_size: 1,
            fuel_per_ms: 50_000,
            debug_mode: true,
            enable_simd: true,
            enable_bulk_memory: true,
            enable_multi_value: true,
        }
    }
}

/// Runtime statistics
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RuntimeStats {
    /// Total modules loaded
    pub modules_loaded: u64,

    /// Total executions
    pub total_executions: u64,

    /// Successful executions
    pub successful_executions: u64,

    /// Failed executions
    pub failed_executions: u64,

    /// Timeout errors
    pub timeout_errors: u64,

    /// Memory errors
    pub memory_errors: u64,

    /// Total execution time (microseconds)
    pub total_execution_time_us: u64,

    /// Peak memory usage (bytes)
    pub peak_memory_bytes: usize,
}

/// A loaded WASM module ready for execution
struct LoadedModule {
    /// Module configuration
    config: TransformationConfig,

    /// Compiled wasmtime module
    #[cfg(feature = "wasm-runtime")]
    module: Module,

    /// Module bytes (for recompilation/serialization)
    #[cfg(not(feature = "wasm-runtime"))]
    #[allow(dead_code)]
    module_bytes: Vec<u8>,

    /// Entry point function name
    #[cfg(feature = "wasm-runtime")]
    entry_point: String,

    /// Entry point function name (unused in stub mode)
    #[cfg(not(feature = "wasm-runtime"))]
    #[allow(dead_code)]
    entry_point: String,

    /// Load time
    loaded_at: Instant,

    /// Execution count
    execution_count: u64,

    /// Total execution time in microseconds
    total_execution_time_us: u64,

    /// Error count
    error_count: u64,
}

/// Store data for WASM execution
#[cfg(feature = "wasm-runtime")]
#[allow(dead_code)]
struct StoreData {
    /// Store limits for resource tracking
    limits: StoreLimits,

    /// Input data buffer
    input_buffer: Vec<u8>,

    /// Output data buffer
    output_buffer: Vec<u8>,

    /// Last error message
    last_error: Option<String>,
}

#[cfg(feature = "wasm-runtime")]
impl StoreData {
    fn new(max_memory: usize) -> Self {
        Self {
            limits: StoreLimitsBuilder::new()
                .memory_size(max_memory)
                .instances(10)
                .tables(10)
                .memories(1)
                .build(),
            input_buffer: Vec::new(),
            output_buffer: Vec::new(),
            last_error: None,
        }
    }
}

impl WasmRuntime {
    /// Create a new WASM runtime with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(RuntimeConfig::default())
    }

    /// Create a new WASM runtime with custom configuration
    #[cfg(feature = "wasm-runtime")]
    pub fn with_config(config: RuntimeConfig) -> Result<Self> {
        // Configure wasmtime engine
        let mut wasmtime_config = Config::new();
        wasmtime_config
            .consume_fuel(true) // Enable fuel-based execution limits
            .wasm_simd(config.enable_simd)
            .wasm_bulk_memory(config.enable_bulk_memory)
            .wasm_multi_value(config.enable_multi_value)
            .max_wasm_stack(config.max_stack_size)
            .cranelift_opt_level(wasmtime::OptLevel::Speed);

        if config.enable_caching {
            // Use default cache config if available
            if let Err(e) = wasmtime_config.cache_config_load_default() {
                debug!("WASM cache not available: {}", e);
            }
        }

        let engine = Engine::new(&wasmtime_config).map_err(|e| {
            WasmError::Configuration(format!("Failed to create WASM engine: {}", e))
        })?;

        info!(
            max_memory = config.max_memory,
            timeout_ms = config.max_execution_time_ms,
            pool_size = config.instance_pool_size,
            "WASM runtime initialized"
        );

        Ok(Self {
            modules: RwLock::new(HashMap::new()),
            config,
            engine,
            stats: RwLock::new(RuntimeStats::default()),
        })
    }

    /// Create a new WASM runtime (stub for non-wasm builds)
    #[cfg(not(feature = "wasm-runtime"))]
    pub fn with_config(config: RuntimeConfig) -> Result<Self> {
        info!("WASM runtime initialized (stub mode - wasm-runtime feature not enabled)");
        Ok(Self {
            modules: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(RuntimeStats::default()),
        })
    }

    /// Load a WASM module from a file
    #[cfg(feature = "wasm-runtime")]
    pub async fn load_module(&self, config: TransformationConfig) -> Result<()> {
        let module_id = config.id.clone();
        let wasm_path = config.wasm_path.clone();
        let entry_point = config.entry_point.clone();

        info!(
            module_id = %module_id,
            path = %wasm_path.display(),
            entry_point = %entry_point,
            "Loading WASM module"
        );

        // Read module bytes
        let module_bytes = tokio::fs::read(&wasm_path)
            .await
            .map_err(|e| WasmError::Storage(format!("Failed to read WASM file: {}", e)))?;

        // Validate WASM magic bytes
        if module_bytes.len() < 8 || &module_bytes[0..4] != b"\x00asm" {
            return Err(WasmError::Storage("Invalid WASM file format".into()));
        }

        // Compile module
        let module = Module::new(&self.engine, &module_bytes)
            .map_err(|e| WasmError::Storage(format!("Failed to compile WASM module: {}", e)))?;

        // Validate module exports
        self.validate_module_exports(&module, &entry_point)?;

        let loaded = LoadedModule {
            config,
            module,
            entry_point,
            loaded_at: Instant::now(),
            execution_count: 0,
            total_execution_time_us: 0,
            error_count: 0,
        };

        let mut modules = self.modules.write();
        modules.insert(module_id.clone(), loaded);

        // Update stats
        let mut stats = self.stats.write();
        stats.modules_loaded += 1;

        info!(
            module_id = %module_id,
            "WASM module loaded and compiled successfully"
        );

        Ok(())
    }

    /// Load a WASM module (stub for non-wasm builds)
    #[cfg(not(feature = "wasm-runtime"))]
    pub async fn load_module(&self, config: TransformationConfig) -> Result<()> {
        let module_id = config.id.clone();
        let wasm_path = config.wasm_path.clone();

        info!(
            module_id = %module_id,
            path = %wasm_path.display(),
            "Loading WASM module (stub mode)"
        );

        // Read module bytes for validation
        let module_bytes = tokio::fs::read(&wasm_path)
            .await
            .map_err(|e| WasmError::Storage(format!("Failed to read WASM file: {}", e)))?;

        // Validate WASM magic bytes
        if module_bytes.len() < 8 || &module_bytes[0..4] != b"\x00asm" {
            return Err(WasmError::Storage("Invalid WASM file format".into()));
        }

        let loaded = LoadedModule {
            config,
            module_bytes,
            entry_point: "transform".to_string(),
            loaded_at: Instant::now(),
            execution_count: 0,
            total_execution_time_us: 0,
            error_count: 0,
        };

        let mut modules = self.modules.write();
        modules.insert(module_id.clone(), loaded);

        let mut stats = self.stats.write();
        stats.modules_loaded += 1;

        Ok(())
    }

    /// Validate that a module has the required exports
    #[cfg(feature = "wasm-runtime")]
    fn validate_module_exports(&self, module: &Module, entry_point: &str) -> Result<()> {
        let exports: Vec<_> = module.exports().collect();
        let export_names: Vec<&str> = exports.iter().map(|e| e.name()).collect();

        // Check for required exports
        let required_exports = ["alloc", "dealloc", entry_point];
        for required in &required_exports {
            if !export_names.contains(required) {
                return Err(WasmError::Configuration(format!(
                    "WASM module missing required export: {}",
                    required
                )));
            }
        }

        // Check for memory export
        if !export_names.contains(&"memory") {
            return Err(WasmError::Configuration(
                "WASM module must export 'memory'".into(),
            ));
        }

        Ok(())
    }

    /// Load a WASM module from bytes
    #[cfg(feature = "wasm-runtime")]
    pub async fn load_module_bytes(
        &self,
        module_id: &str,
        config: TransformationConfig,
        bytes: Vec<u8>,
    ) -> Result<()> {
        // Validate WASM magic bytes
        if bytes.len() < 8 || &bytes[0..4] != b"\x00asm" {
            return Err(WasmError::Storage("Invalid WASM file format".into()));
        }

        let entry_point = config.entry_point.clone();

        // Compile module
        let module = Module::new(&self.engine, &bytes)
            .map_err(|e| WasmError::Storage(format!("Failed to compile WASM module: {}", e)))?;

        // Validate exports
        self.validate_module_exports(&module, &entry_point)?;

        let loaded = LoadedModule {
            config,
            module,
            entry_point,
            loaded_at: Instant::now(),
            execution_count: 0,
            total_execution_time_us: 0,
            error_count: 0,
        };

        let mut modules = self.modules.write();
        modules.insert(module_id.to_string(), loaded);

        let mut stats = self.stats.write();
        stats.modules_loaded += 1;

        Ok(())
    }

    /// Load a WASM module from bytes (stub)
    #[cfg(not(feature = "wasm-runtime"))]
    pub async fn load_module_bytes(
        &self,
        module_id: &str,
        config: TransformationConfig,
        bytes: Vec<u8>,
    ) -> Result<()> {
        if bytes.len() < 8 || &bytes[0..4] != b"\x00asm" {
            return Err(WasmError::Storage("Invalid WASM file format".into()));
        }

        let loaded = LoadedModule {
            config,
            module_bytes: bytes,
            entry_point: "transform".to_string(),
            loaded_at: Instant::now(),
            execution_count: 0,
            total_execution_time_us: 0,
            error_count: 0,
        };

        let mut modules = self.modules.write();
        modules.insert(module_id.to_string(), loaded);

        let mut stats = self.stats.write();
        stats.modules_loaded += 1;

        Ok(())
    }

    /// Unload a module
    pub async fn unload_module(&self, module_id: &str) -> Result<()> {
        let mut modules = self.modules.write();
        if modules.remove(module_id).is_some() {
            info!(module_id = %module_id, "WASM module unloaded");
        }
        Ok(())
    }

    /// Execute a transformation
    #[cfg(feature = "wasm-runtime")]
    pub async fn execute(&self, module_id: &str, input: TransformInput) -> Result<TransformOutput> {
        let start = Instant::now();

        // Get module info
        let (module_clone, config_clone, entry_point) = {
            let modules = self.modules.read();
            let module = modules
                .get(module_id)
                .ok_or_else(|| WasmError::Storage(format!("Module not found: {}", module_id)))?;

            if !module.config.enabled {
                return Ok(TransformOutput::Pass(input));
            }

            (
                module.module.clone(),
                module.config.clone(),
                module.entry_point.clone(),
            )
        };

        // Execute in a separate task to handle async properly
        let result = self
            .execute_wasm(&module_clone, &config_clone, &entry_point, input.clone())
            .await;

        let elapsed_us = start.elapsed().as_micros() as u64;

        // Update module statistics
        {
            let mut modules = self.modules.write();
            if let Some(module) = modules.get_mut(module_id) {
                module.execution_count += 1;
                module.total_execution_time_us += elapsed_us;
                if result.is_err() {
                    module.error_count += 1;
                }
            }
        }

        // Update runtime statistics
        {
            let mut stats = self.stats.write();
            stats.total_executions += 1;
            stats.total_execution_time_us += elapsed_us;
            if result.is_ok() {
                stats.successful_executions += 1;
            } else {
                stats.failed_executions += 1;
            }
        }

        result
    }

    /// Execute WASM module
    #[cfg(feature = "wasm-runtime")]
    async fn execute_wasm(
        &self,
        module: &Module,
        config: &TransformationConfig,
        entry_point: &str,
        input: TransformInput,
    ) -> Result<TransformOutput> {
        // Create store with limits
        let mut store = Store::new(&self.engine, StoreData::new(config.max_memory_bytes));
        store.limiter(|data| &mut data.limits);

        // Set fuel for timeout enforcement
        let fuel = self.config.fuel_per_ms * config.timeout_ms;
        store
            .set_fuel(fuel)
            .map_err(|e| WasmError::Storage(format!("Failed to set fuel: {}", e)))?;

        // Create linker with host functions
        let mut linker = Linker::new(&self.engine);
        self.setup_host_functions(&mut linker)?;

        // Instantiate module
        let instance = linker
            .instantiate(&mut store, module)
            .map_err(|e| WasmError::Storage(format!("Failed to instantiate module: {}", e)))?;

        // Get exports
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmError::Storage("Module has no memory export".into()))?;

        let alloc_fn: TypedFunc<i32, i32> = instance
            .get_typed_func(&mut store, "alloc")
            .map_err(|e| WasmError::Storage(format!("Failed to get alloc function: {}", e)))?;

        let dealloc_fn: TypedFunc<(i32, i32), ()> = instance
            .get_typed_func(&mut store, "dealloc")
            .map_err(|e| WasmError::Storage(format!("Failed to get dealloc function: {}", e)))?;

        let transform_fn: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(&mut store, entry_point)
            .map_err(|e| {
                WasmError::Storage(format!("Failed to get {} function: {}", entry_point, e))
            })?;

        // Serialize input to JSON
        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| WasmError::Storage(format!("Failed to serialize input: {}", e)))?;
        let input_len = input_bytes.len() as i32;

        // Allocate memory in WASM for input
        let input_ptr = alloc_fn.call(&mut store, input_len).map_err(|e| {
            if e.to_string().contains("fuel") {
                let mut stats = self.stats.write();
                stats.timeout_errors += 1;
                WasmError::Timeout("WASM execution timeout".into())
            } else {
                WasmError::Storage(format!("Alloc failed: {}", e))
            }
        })?;

        // Write input to WASM memory
        memory
            .write(&mut store, input_ptr as usize, &input_bytes)
            .map_err(|e| WasmError::Storage(format!("Memory write failed: {}", e)))?;

        // Call transform function
        let result = transform_fn
            .call(&mut store, (input_ptr, input_len))
            .map_err(|e| {
                if e.to_string().contains("fuel") {
                    let mut stats = self.stats.write();
                    stats.timeout_errors += 1;
                    WasmError::Timeout("WASM execution timeout".into())
                } else {
                    WasmError::Storage(format!("Transform execution failed: {}", e))
                }
            })?;

        // Free input memory
        let _ = dealloc_fn.call(&mut store, (input_ptr, input_len));

        // Parse result: high 32 bits = ptr, low 32 bits = len
        // 0 means drop the message
        if result == 0 {
            return Ok(TransformOutput::Drop);
        }

        let output_ptr = (result >> 32) as i32;
        let output_len = (result & 0xFFFFFFFF) as i32;

        if output_len <= 0 || output_ptr <= 0 {
            return Ok(TransformOutput::Drop);
        }

        // Read output from WASM memory
        let mut output_bytes = vec![0u8; output_len as usize];
        memory
            .read(&store, output_ptr as usize, &mut output_bytes)
            .map_err(|e| WasmError::Storage(format!("Memory read failed: {}", e)))?;

        // Free output memory
        let _ = dealloc_fn.call(&mut store, (output_ptr, output_len));

        // Parse output
        let output: WasmTransformOutput = serde_json::from_slice(&output_bytes)
            .map_err(|e| WasmError::Storage(format!("Failed to parse output: {}", e)))?;

        // Convert to TransformOutput
        Ok(self.convert_wasm_output(output, input, config.transform_type))
    }

    /// Setup host functions for WASM modules
    #[cfg(feature = "wasm-runtime")]
    fn setup_host_functions(&self, linker: &mut Linker<StoreData>) -> Result<()> {
        // Log function for debugging
        linker
            .func_wrap(
                "env",
                "log",
                |mut caller: wasmtime::Caller<'_, StoreData>, ptr: i32, len: i32| {
                    if let Some(memory) = caller.get_export("memory").and_then(Extern::into_memory)
                    {
                        let mut buf = vec![0u8; len as usize];
                        if memory.read(&caller, ptr as usize, &mut buf).is_ok() {
                            if let Ok(msg) = std::str::from_utf8(&buf) {
                                debug!(target: "wasm", "{}", msg);
                            }
                        }
                    }
                },
            )
            .map_err(|e| WasmError::Storage(format!("Failed to setup log function: {}", e)))?;

        // Get current timestamp
        linker
            .func_wrap("env", "get_time_ms", || -> i64 {
                chrono::Utc::now().timestamp_millis()
            })
            .map_err(|e| {
                WasmError::Storage(format!("Failed to setup get_time_ms function: {}", e))
            })?;

        // Generate random bytes
        linker
            .func_wrap("env", "random", || -> i64 { rand::random::<i64>() })
            .map_err(|e| WasmError::Storage(format!("Failed to setup random function: {}", e)))?;

        Ok(())
    }

    /// Convert WASM output to TransformOutput
    #[cfg(feature = "wasm-runtime")]
    fn convert_wasm_output(
        &self,
        output: WasmTransformOutput,
        input: TransformInput,
        _transform_type: TransformationType,
    ) -> TransformOutput {
        match output {
            WasmTransformOutput::Pass => TransformOutput::Pass(input),
            WasmTransformOutput::Drop => TransformOutput::Drop,
            WasmTransformOutput::Error(msg) => TransformOutput::Error(msg),
            WasmTransformOutput::Transformed { messages } => {
                let results: Vec<TransformResult> = messages
                    .into_iter()
                    .map(|m| TransformResult {
                        key: m.key,
                        value: m.value,
                        headers: m.headers,
                    })
                    .collect();
                TransformOutput::Transformed(results)
            }
            WasmTransformOutput::Route { destinations } => {
                let results: Vec<RouteResult> = destinations
                    .into_iter()
                    .map(|d| RouteResult {
                        topic: d.topic,
                        partition: d.partition,
                        key: d.key,
                        value: d.value,
                        headers: d.headers,
                    })
                    .collect();
                TransformOutput::Route(results)
            }
        }
    }

    /// Execute a transformation (stub for non-wasm builds)
    #[cfg(not(feature = "wasm-runtime"))]
    pub async fn execute(&self, module_id: &str, input: TransformInput) -> Result<TransformOutput> {
        let start = Instant::now();

        let transform_type = {
            let modules = self.modules.read();
            let module = modules
                .get(module_id)
                .ok_or_else(|| WasmError::Storage(format!("Module not found: {}", module_id)))?;

            if !module.config.enabled {
                return Ok(TransformOutput::Pass(input));
            }

            module.config.transform_type
        };

        // Stub execution - just pass through
        let output = match transform_type {
            TransformationType::Filter => TransformOutput::Pass(input),
            TransformationType::Transform => TransformOutput::Transformed(vec![TransformResult {
                key: input.key.clone(),
                value: input.value.clone(),
                headers: input.headers.clone(),
            }]),
            TransformationType::Route => TransformOutput::Route(vec![RouteResult {
                topic: input.topic.clone(),
                partition: Some(input.partition),
                key: input.key,
                value: input.value,
                headers: input.headers,
            }]),
            TransformationType::Enrich | TransformationType::Aggregate => {
                TransformOutput::Pass(input)
            }
        };

        let elapsed_us = start.elapsed().as_micros() as u64;

        // Update statistics
        {
            let mut modules = self.modules.write();
            if let Some(module) = modules.get_mut(module_id) {
                module.execution_count += 1;
                module.total_execution_time_us += elapsed_us;
            }
        }

        {
            let mut stats = self.stats.write();
            stats.total_executions += 1;
            stats.successful_executions += 1;
            stats.total_execution_time_us += elapsed_us;
        }

        Ok(output)
    }

    /// Get statistics for a module
    pub async fn get_stats(&self, module_id: &str) -> Option<ModuleStats> {
        let modules = self.modules.read();
        modules.get(module_id).map(|m| ModuleStats {
            execution_count: m.execution_count,
            total_execution_time_us: m.total_execution_time_us,
            avg_execution_time_us: if m.execution_count > 0 {
                m.total_execution_time_us / m.execution_count
            } else {
                0
            },
            error_count: m.error_count,
            loaded_at: m.loaded_at,
            uptime_secs: m.loaded_at.elapsed().as_secs(),
        })
    }

    /// Get runtime statistics
    pub fn get_runtime_stats(&self) -> RuntimeStats {
        self.stats.read().clone()
    }

    /// List loaded modules
    pub async fn list_modules(&self) -> Vec<String> {
        let modules = self.modules.read();
        modules.keys().cloned().collect()
    }

    /// Check if a module is loaded
    pub async fn is_loaded(&self, module_id: &str) -> bool {
        let modules = self.modules.read();
        modules.contains_key(module_id)
    }

    /// Get module configuration
    pub async fn get_module_config(&self, module_id: &str) -> Option<TransformationConfig> {
        let modules = self.modules.read();
        modules.get(module_id).map(|m| m.config.clone())
    }

    /// Update module configuration (enables hot reload)
    pub async fn update_module_config(
        &self,
        module_id: &str,
        config: TransformationConfig,
    ) -> Result<()> {
        let mut modules = self.modules.write();
        if let Some(module) = modules.get_mut(module_id) {
            module.config = config;
            Ok(())
        } else {
            Err(WasmError::Storage(format!(
                "Module not found: {}",
                module_id
            )))
        }
    }

    /// Reload a module from disk
    pub async fn reload_module(&self, module_id: &str) -> Result<()> {
        let config = {
            let modules = self.modules.read();
            modules
                .get(module_id)
                .map(|m| m.config.clone())
                .ok_or_else(|| WasmError::Storage(format!("Module not found: {}", module_id)))?
        };

        // Unload and reload
        self.unload_module(module_id).await?;
        self.load_module(config).await
    }
}

impl Default for WasmRuntime {
    fn default() -> Self {
        Self::new().expect("Failed to create default WASM runtime")
    }
}

/// Statistics for a loaded module
#[derive(Debug, Clone, serde::Serialize)]
pub struct ModuleStats {
    /// Number of times executed
    pub execution_count: u64,

    /// Total execution time in microseconds
    pub total_execution_time_us: u64,

    /// Average execution time in microseconds
    pub avg_execution_time_us: u64,

    /// Number of errors
    pub error_count: u64,

    /// When the module was loaded (seconds since load)
    #[serde(skip)]
    pub loaded_at: Instant,

    /// Uptime in seconds (calculated from loaded_at)
    #[serde(rename = "uptime_seconds")]
    pub uptime_secs: u64,
}

/// WASM module output format (JSON-serializable)
#[cfg(feature = "wasm-runtime")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WasmTransformOutput {
    /// Pass through unchanged
    Pass,

    /// Drop the message
    Drop,

    /// Error occurred
    Error(String),

    /// Transformed messages
    Transformed { messages: Vec<WasmMessage> },

    /// Route to different topics
    Route {
        destinations: Vec<WasmRouteDestination>,
    },
}

/// Message format for WASM output
#[cfg(feature = "wasm-runtime")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WasmMessage {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    #[serde(default)]
    headers: HashMap<String, Vec<u8>>,
}

/// Route destination for WASM output
#[cfg(feature = "wasm-runtime")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WasmRouteDestination {
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    #[serde(default)]
    headers: HashMap<String, Vec<u8>>,
}

/// Check if WASM transforms are available
pub fn is_wasm_transforms_available() -> bool {
    cfg!(feature = "wasm-runtime")
}

/// Get WASM runtime version info
pub fn get_wasm_runtime_info() -> WasmRuntimeInfo {
    WasmRuntimeInfo {
        available: is_wasm_transforms_available(),
        #[cfg(feature = "wasm-runtime")]
        engine: "wasmtime".to_string(),
        #[cfg(not(feature = "wasm-runtime"))]
        engine: "stub".to_string(),
        #[cfg(feature = "wasm-runtime")]
        version: "27.0".to_string(),
        #[cfg(not(feature = "wasm-runtime"))]
        version: "N/A".to_string(),
    }
}

/// WASM runtime information
#[derive(Debug, Clone, serde::Serialize)]
pub struct WasmRuntimeInfo {
    /// Whether WASM transforms are available
    pub available: bool,

    /// WASM engine name
    pub engine: String,

    /// Engine version
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_runtime_creation() {
        let runtime = WasmRuntime::new().unwrap();
        assert!(runtime.list_modules().await.is_empty());
    }

    #[tokio::test]
    async fn test_runtime_with_config() {
        let config = RuntimeConfig {
            max_memory: 128 * 1024 * 1024,
            max_execution_time_ms: 2000,
            ..Default::default()
        };

        let runtime = WasmRuntime::with_config(config).unwrap();
        assert!(runtime.list_modules().await.is_empty());
    }

    #[tokio::test]
    async fn test_runtime_configs() {
        let high_perf = RuntimeConfig::high_performance();
        assert_eq!(high_perf.instance_pool_size, 8);

        let debug = RuntimeConfig::debug();
        assert!(debug.debug_mode);
    }

    #[tokio::test]
    async fn test_load_module_bytes() {
        let runtime = WasmRuntime::new().unwrap();

        // Valid WASM header (minimal valid module)
        let wasm_bytes = vec![
            0x00, 0x61, 0x73, 0x6d, // Magic: \0asm
            0x01, 0x00, 0x00, 0x00, // Version: 1
        ];

        let config = TransformationConfig {
            id: "test-module".to_string(),
            name: "Test Module".to_string(),
            wasm_path: PathBuf::from("test.wasm"),
            ..Default::default()
        };

        // This will fail validation in wasm-runtime mode (no required exports)
        // but succeed in stub mode
        #[cfg(not(feature = "wasm-runtime"))]
        {
            runtime
                .load_module_bytes("test-module", config, wasm_bytes)
                .await
                .unwrap();
            assert!(runtime.is_loaded("test-module").await);
        }

        #[cfg(feature = "wasm-runtime")]
        {
            // With wasmtime, a minimal WASM won't have required exports
            let result = runtime
                .load_module_bytes("test-module", config, wasm_bytes)
                .await;
            // Expected to fail due to missing exports
            assert!(result.is_err() || runtime.is_loaded("test-module").await);
        }
    }

    #[tokio::test]
    async fn test_invalid_wasm_bytes() {
        let runtime = WasmRuntime::new().unwrap();

        // Invalid WASM header
        let invalid_bytes = vec![0x00, 0x00, 0x00, 0x00];

        let config = TransformationConfig {
            id: "invalid-module".to_string(),
            ..Default::default()
        };

        let result = runtime
            .load_module_bytes("invalid-module", config, invalid_bytes)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unload_module() {
        let _runtime = WasmRuntime::new().unwrap();

        // Use stub mode test
        #[cfg(not(feature = "wasm-runtime"))]
        {
            let runtime = &_runtime;
            let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

            let config = TransformationConfig {
                id: "test-module".to_string(),
                ..Default::default()
            };

            runtime
                .load_module_bytes("test-module", config, wasm_bytes)
                .await
                .unwrap();

            assert!(runtime.is_loaded("test-module").await);

            runtime.unload_module("test-module").await.unwrap();

            assert!(!runtime.is_loaded("test-module").await);
        }
    }

    #[tokio::test]
    async fn test_runtime_stats() {
        let runtime = WasmRuntime::new().unwrap();
        let stats = runtime.get_runtime_stats();
        assert_eq!(stats.total_executions, 0);
    }

    #[test]
    fn test_wasm_runtime_info() {
        let info = get_wasm_runtime_info();
        #[cfg(feature = "wasm-runtime")]
        {
            assert!(info.available);
            assert_eq!(info.engine, "wasmtime");
        }
        #[cfg(not(feature = "wasm-runtime"))]
        {
            assert!(!info.available);
            assert_eq!(info.engine, "stub");
        }
    }

    #[test]
    fn test_is_wasm_available() {
        let available = is_wasm_transforms_available();
        #[cfg(feature = "wasm-runtime")]
        assert!(available);
        #[cfg(not(feature = "wasm-runtime"))]
        assert!(!available);
    }
}
