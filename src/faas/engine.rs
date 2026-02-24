//! FaaS execution engine — orchestrates function invocations.

use super::function::{FaasFunction, FunctionConfig, FunctionState, InvocationResult};
use super::registry::{FunctionInfo, FunctionRegistry};
use super::trigger::{TriggerBinding, TriggerType};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, error, info, warn};

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

/// The FaaS execution engine.
pub struct FaasEngine {
    config: FaasEngineConfig,
    registry: FunctionRegistry,
    total_invocations: AtomicU64,
    active_invocations: AtomicU64,
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
            .ok_or_else(|| StreamlineError::Config(format!("Function '{}' not found", name)))?;

        match func.state {
            FunctionState::Inactive | FunctionState::Paused | FunctionState::Error => {
                func.set_state(FunctionState::Active);
                info!(function = name, "Function started");
                Ok(())
            }
            FunctionState::Active => Ok(()),
            other => Err(StreamlineError::Config(format!(
                "Cannot start function in state {:?}",
                other
            ))),
        }
    }

    /// Stop a function (transition to Inactive).
    pub fn stop_function(&mut self, name: &str) -> Result<()> {
        let func = self
            .registry
            .get_function_mut(name)
            .ok_or_else(|| StreamlineError::Config(format!("Function '{}' not found", name)))?;

        func.set_state(FunctionState::Inactive);
        info!(function = name, "Function stopped");
        Ok(())
    }

    /// Invoke a function with input data.
    ///
    /// This is the core execution path. In production, this would:
    /// 1. Load the WASM module (cached after first load)
    /// 2. Create a WASM instance with resource limits
    /// 3. Call the entry point with the input data
    /// 4. Collect output and return the result
    pub fn invoke(
        &self,
        function_name: &str,
        input: &[u8],
    ) -> Result<InvocationResult> {
        let func = self
            .registry
            .get_function(function_name)
            .ok_or_else(|| {
                StreamlineError::Config(format!("Function '{}' not found", function_name))
            })?;

        if func.state != FunctionState::Active {
            return Err(StreamlineError::Config(format!(
                "Function '{}' is not active (state: {:?})",
                function_name, func.state
            )));
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

        // Execute the function (WASM runtime integration point)
        let result = self.execute_wasm(func, input);

        self.active_invocations.fetch_sub(1, Ordering::Relaxed);
        self.total_invocations.fetch_add(1, Ordering::Relaxed);

        let execution_ms = start.elapsed().as_millis() as u64;

        let invocation_result = match result {
            Ok(output) => InvocationResult {
                success: true,
                output: Some(output),
                error: None,
                execution_ms,
                retries: 0,
            },
            Err(e) => InvocationResult {
                success: false,
                output: None,
                error: Some(e.to_string()),
                execution_ms,
                retries: 0,
            },
        };

        func.record_invocation(&invocation_result);
        Ok(invocation_result)
    }

    /// Execute a WASM function.
    ///
    /// This is the integration point with the WASM runtime (`wasmtime`).
    /// Without the WASM runtime linked, returns the input as-is (passthrough).
    fn execute_wasm(&self, func: &FaasFunction, input: &[u8]) -> Result<Vec<u8>> {
        // The WASM execution sequence:
        //   1. Get or compile WASM module from func.config.wasm_source
        //   2. Create instance with:
        //      - Memory limit: func.config.limits.max_memory_bytes
        //      - Fuel limit (for CPU time): func.config.limits.max_execution_ms
        //      - Environment variables: func.config.env_vars
        //   3. Call entry point: func.config.entry_point(input) -> output
        //   4. Collect output bytes
        //   5. Check for traps (out of fuel, memory, etc.)

        debug!(
            function = %func.config.name,
            entry = %func.config.entry_point,
            input_size = input.len(),
            "Executing WASM function (passthrough mode — runtime not linked)"
        );

        // Passthrough: return input as output when WASM runtime is not available
        Ok(input.to_vec())
    }

    /// Invoke with retries.
    pub fn invoke_with_retries(
        &self,
        function_name: &str,
        input: &[u8],
    ) -> Result<InvocationResult> {
        let func = self
            .registry
            .get_function(function_name)
            .ok_or_else(|| {
                StreamlineError::Config(format!("Function '{}' not found", function_name))
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

        // If still failing after retries, this is the final result
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faas::function::{ResourceLimits, WasmSource};

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

        let result = engine.invoke("fn-a", b"hello world").unwrap();
        assert!(result.success);
        assert_eq!(result.output, Some(b"hello world".to_vec()));

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
            max_global_concurrency: 0, // zero = no invocations allowed
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
}
