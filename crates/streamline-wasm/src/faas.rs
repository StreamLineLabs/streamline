//! Streamline Functions-as-a-Service (FaaS)
//!
//! Provides a serverless functions platform built on the WASM runtime.
//!
//! # Features
//!
//! - Deploy functions from source code (Rust, Go, JavaScript, Python)
//! - Auto-scaling based on load
//! - Trigger-based invocation (topic, HTTP, schedule)
//! - Cold start optimization
//! - Function composition and chaining
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────┐
//! │                     STREAMLINE FAAS                                        │
//! ├────────────────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────────┐   │
//! │  │   Function   │    │   Trigger    │    │   Execution                │   │
//! │  │   Registry   │    │   Manager    │    │   Engine                   │   │
//! │  └──────┬───────┘    └──────┬───────┘    └──────────┬─────────────────┘   │
//! │         │                   │                       │                      │
//! │         └───────────────────┼───────────────────────┘                      │
//! │                             │                                              │
//! │  ┌──────────────────────────┴──────────────────────────────────────────┐  │
//! │  │                    WASM Runtime Pool                                 │  │
//! │  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐    │  │
//! │  │  │ Instance 1 │  │ Instance 2 │  │ Instance 3 │  │ Instance N │    │  │
//! │  │  │ (warm)     │  │ (warm)     │  │ (cold)     │  │ (scaling)  │    │  │
//! │  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘    │  │
//! │  └─────────────────────────────────────────────────────────────────────┘  │
//! └────────────────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, WasmError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

/// FaaS Manager
pub struct FaasManager {
    config: FaasConfig,
    functions: Arc<RwLock<HashMap<String, Function>>>,
    instances: Arc<RwLock<HashMap<String, Vec<FunctionInstance>>>>,
    triggers: Arc<RwLock<HashMap<String, Vec<Trigger>>>>,
    stats: Arc<FaasStats>,
}

impl FaasManager {
    /// Create a new FaaS manager
    pub fn new(config: FaasConfig) -> Self {
        Self {
            config,
            functions: Arc::new(RwLock::new(HashMap::new())),
            instances: Arc::new(RwLock::new(HashMap::new())),
            triggers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(FaasStats::default()),
        }
    }

    /// Deploy a new function
    pub async fn deploy_function(&self, spec: FunctionSpec) -> Result<Function> {
        // Validate spec
        self.validate_spec(&spec)?;

        let function = Function {
            id: Uuid::new_v4().to_string(),
            name: spec.name.clone(),
            runtime: spec.runtime,
            code: spec.code,
            handler: spec.handler,
            memory_mb: spec.memory_mb.unwrap_or(128),
            timeout_seconds: spec.timeout_seconds.unwrap_or(30),
            environment: spec.environment,
            state: FunctionState::Deployed,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
            config: spec.config,
        };

        // Store function
        let mut functions = self.functions.write().await;
        if functions.values().any(|f| f.name == spec.name) {
            return Err(WasmError::Configuration(format!(
                "Function already exists: {}",
                spec.name
            )));
        }
        functions.insert(function.id.clone(), function.clone());

        self.stats
            .functions_deployed
            .fetch_add(1, Ordering::Relaxed);
        info!("Deployed function: {} ({})", function.name, function.id);

        Ok(function)
    }

    /// Validate function spec
    fn validate_spec(&self, spec: &FunctionSpec) -> Result<()> {
        if spec.name.is_empty() {
            return Err(WasmError::Configuration(
                "Function name is required".to_string(),
            ));
        }
        if spec.code.is_empty() {
            return Err(WasmError::Configuration(
                "Function code is required".to_string(),
            ));
        }
        if spec.handler.is_empty() {
            return Err(WasmError::Configuration(
                "Function handler is required".to_string(),
            ));
        }
        Ok(())
    }

    /// Update an existing function
    pub async fn update_function(&self, function_id: &str, spec: FunctionSpec) -> Result<Function> {
        let mut functions = self.functions.write().await;
        let function = functions.get_mut(function_id).ok_or_else(|| {
            WasmError::Configuration(format!("Function not found: {}", function_id))
        })?;

        function.code = spec.code;
        function.handler = spec.handler;
        if let Some(memory) = spec.memory_mb {
            function.memory_mb = memory;
        }
        if let Some(timeout) = spec.timeout_seconds {
            function.timeout_seconds = timeout;
        }
        function.environment = spec.environment;
        function.config = spec.config;
        function.version += 1;
        function.updated_at = Utc::now();

        // Clear warm instances on update
        drop(functions);
        let mut instances = self.instances.write().await;
        instances.remove(function_id);

        let functions = self.functions.read().await;
        let function = functions.get(function_id).unwrap();

        info!(
            "Updated function: {} to version {}",
            function.name, function.version
        );
        Ok(function.clone())
    }

    /// Delete a function
    pub async fn delete_function(&self, function_id: &str) -> Result<()> {
        let mut functions = self.functions.write().await;
        if functions.remove(function_id).is_none() {
            return Err(WasmError::Configuration(format!(
                "Function not found: {}",
                function_id
            )));
        }

        // Remove instances and triggers
        drop(functions);
        let mut instances = self.instances.write().await;
        instances.remove(function_id);
        drop(instances);

        let mut triggers = self.triggers.write().await;
        triggers.remove(function_id);

        self.stats.functions_deleted.fetch_add(1, Ordering::Relaxed);
        info!("Deleted function: {}", function_id);
        Ok(())
    }

    /// Get a function by ID
    pub async fn get_function(&self, function_id: &str) -> Option<Function> {
        let functions = self.functions.read().await;
        functions.get(function_id).cloned()
    }

    /// Get a function by name
    pub async fn get_function_by_name(&self, name: &str) -> Option<Function> {
        let functions = self.functions.read().await;
        functions.values().find(|f| f.name == name).cloned()
    }

    /// List all functions
    pub async fn list_functions(&self) -> Vec<FunctionSummary> {
        let functions = self.functions.read().await;
        functions
            .values()
            .map(|f| FunctionSummary {
                id: f.id.clone(),
                name: f.name.clone(),
                runtime: f.runtime,
                state: f.state.clone(),
                version: f.version,
                created_at: f.created_at,
            })
            .collect()
    }

    /// Invoke a function
    pub async fn invoke(
        &self,
        function_id: &str,
        request: InvocationRequest,
    ) -> Result<InvocationResult> {
        let start = Instant::now();
        self.stats.invocations.fetch_add(1, Ordering::Relaxed);

        // Get function
        let functions = self.functions.read().await;
        let function = functions.get(function_id).ok_or_else(|| {
            WasmError::Configuration(format!("Function not found: {}", function_id))
        })?;

        if function.state != FunctionState::Deployed {
            return Err(WasmError::Configuration(format!(
                "Function {} is not in deployed state",
                function.name
            )));
        }

        let function = function.clone();
        drop(functions);

        // Get or create instance
        let instance = self.get_or_create_instance(&function).await?;

        // Execute function
        let execution_result = self.execute_instance(&function, &instance, &request).await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match execution_result {
            Ok(output) => {
                self.stats
                    .successful_invocations
                    .fetch_add(1, Ordering::Relaxed);
                Ok(InvocationResult {
                    request_id: Uuid::new_v4().to_string(),
                    function_id: function_id.to_string(),
                    output,
                    duration_ms,
                    status: InvocationStatus::Success,
                    error: None,
                    logs: Vec::new(),
                })
            }
            Err(e) => {
                self.stats
                    .failed_invocations
                    .fetch_add(1, Ordering::Relaxed);
                Ok(InvocationResult {
                    request_id: Uuid::new_v4().to_string(),
                    function_id: function_id.to_string(),
                    output: serde_json::Value::Null,
                    duration_ms,
                    status: InvocationStatus::Failed,
                    error: Some(e.to_string()),
                    logs: Vec::new(),
                })
            }
        }
    }

    /// Get or create a function instance
    async fn get_or_create_instance(&self, function: &Function) -> Result<FunctionInstance> {
        let mut instances = self.instances.write().await;
        let function_instances = instances.entry(function.id.clone()).or_default();

        // Try to get a warm instance
        if let Some(instance) = function_instances
            .iter_mut()
            .find(|i| i.state == InstanceState::Warm)
        {
            instance.last_used = Utc::now();
            instance.invocation_count += 1;
            return Ok(instance.clone());
        }

        // Check if we can create a new instance
        if function_instances.len() >= self.config.max_instances_per_function {
            // Wait for an instance or return error
            return Err(WasmError::ResourceExhausted(
                "Max instances reached for function".to_string(),
            ));
        }

        // Create cold instance
        self.stats.cold_starts.fetch_add(1, Ordering::Relaxed);
        let instance = FunctionInstance {
            id: Uuid::new_v4().to_string(),
            function_id: function.id.clone(),
            state: InstanceState::Warm,
            created_at: Utc::now(),
            last_used: Utc::now(),
            invocation_count: 1,
        };

        function_instances.push(instance.clone());
        Ok(instance)
    }

    /// Execute a function instance
    async fn execute_instance(
        &self,
        function: &Function,
        _instance: &FunctionInstance,
        request: &InvocationRequest,
    ) -> Result<serde_json::Value> {
        // In a real implementation, this would execute the WASM module
        // For now, we simulate execution

        debug!(
            "Executing function {} with handler {}",
            function.name, function.handler
        );

        // Simulate processing based on runtime
        match function.runtime {
            Runtime::Rust => {
                // Rust functions are compiled to WASM
                Ok(serde_json::json!({
                    "status": "executed",
                    "runtime": "rust",
                    "input": request.payload,
                }))
            }
            Runtime::JavaScript => {
                // JavaScript functions run in QuickJS or similar
                Ok(serde_json::json!({
                    "status": "executed",
                    "runtime": "javascript",
                    "input": request.payload,
                }))
            }
            Runtime::Python => {
                // Python functions run in RustPython or similar
                Ok(serde_json::json!({
                    "status": "executed",
                    "runtime": "python",
                    "input": request.payload,
                }))
            }
            Runtime::Go => {
                // Go functions compiled to WASM via TinyGo
                Ok(serde_json::json!({
                    "status": "executed",
                    "runtime": "go",
                    "input": request.payload,
                }))
            }
            Runtime::Wasm => {
                // Pre-compiled WASM
                Ok(serde_json::json!({
                    "status": "executed",
                    "runtime": "wasm",
                    "input": request.payload,
                }))
            }
        }
    }

    /// Add a trigger for a function
    pub async fn add_trigger(
        &self,
        function_id: &str,
        trigger_spec: TriggerSpec,
    ) -> Result<Trigger> {
        // Verify function exists
        let functions = self.functions.read().await;
        if !functions.contains_key(function_id) {
            return Err(WasmError::Configuration(format!(
                "Function not found: {}",
                function_id
            )));
        }
        drop(functions);

        let trigger = Trigger {
            id: Uuid::new_v4().to_string(),
            function_id: function_id.to_string(),
            trigger_type: trigger_spec.trigger_type,
            config: trigger_spec.config,
            enabled: true,
            created_at: Utc::now(),
        };

        let mut triggers = self.triggers.write().await;
        let function_triggers = triggers.entry(function_id.to_string()).or_default();
        function_triggers.push(trigger.clone());

        info!(
            "Added {:?} trigger for function {}",
            trigger.trigger_type, function_id
        );
        Ok(trigger)
    }

    /// Remove a trigger
    pub async fn remove_trigger(&self, function_id: &str, trigger_id: &str) -> Result<()> {
        let mut triggers = self.triggers.write().await;
        let function_triggers = triggers.get_mut(function_id).ok_or_else(|| {
            WasmError::Configuration(format!("No triggers for function: {}", function_id))
        })?;

        let len_before = function_triggers.len();
        function_triggers.retain(|t| t.id != trigger_id);

        if function_triggers.len() == len_before {
            return Err(WasmError::Configuration(format!(
                "Trigger not found: {}",
                trigger_id
            )));
        }

        info!(
            "Removed trigger {} from function {}",
            trigger_id, function_id
        );
        Ok(())
    }

    /// List triggers for a function
    pub async fn list_triggers(&self, function_id: &str) -> Vec<Trigger> {
        let triggers = self.triggers.read().await;
        triggers.get(function_id).cloned().unwrap_or_default()
    }

    /// Process a topic trigger
    pub async fn process_topic_trigger(
        &self,
        topic: &str,
        message: &[u8],
    ) -> Result<Vec<InvocationResult>> {
        let mut results = Vec::new();

        // First, collect the matching function IDs
        let matching_functions: Vec<(String, TriggerType)> = {
            let triggers = self.triggers.read().await;
            let mut matches = Vec::new();
            for (function_id, function_triggers) in triggers.iter() {
                for trigger in function_triggers {
                    if !trigger.enabled {
                        continue;
                    }

                    if let TriggerType::Topic = trigger.trigger_type {
                        if let Some(trigger_topic) =
                            trigger.config.get("topic").and_then(|v| v.as_str())
                        {
                            if trigger_topic == topic || glob_match(trigger_topic, topic) {
                                matches.push((function_id.clone(), TriggerType::Topic));
                                break;
                            }
                        }
                    }
                }
            }
            matches
        };

        // Now invoke with the lock released
        for (function_id, _trigger_type) in matching_functions {
            let request = InvocationRequest {
                payload: serde_json::json!({
                    "topic": topic,
                    "message": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, message),
                }),
                context: InvocationContext {
                    trigger_type: TriggerType::Topic,
                    source: topic.to_string(),
                },
            };

            let result = self.invoke(&function_id, request).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Get FaaS statistics
    pub fn stats(&self) -> FaasStatsSnapshot {
        FaasStatsSnapshot {
            functions_deployed: self.stats.functions_deployed.load(Ordering::Relaxed),
            functions_deleted: self.stats.functions_deleted.load(Ordering::Relaxed),
            invocations: self.stats.invocations.load(Ordering::Relaxed),
            successful_invocations: self.stats.successful_invocations.load(Ordering::Relaxed),
            failed_invocations: self.stats.failed_invocations.load(Ordering::Relaxed),
            cold_starts: self.stats.cold_starts.load(Ordering::Relaxed),
        }
    }

    /// Scale instances for a function
    pub async fn scale_function(&self, function_id: &str, target_instances: usize) -> Result<()> {
        let functions = self.functions.read().await;
        let function = functions.get(function_id).ok_or_else(|| {
            WasmError::Configuration(format!("Function not found: {}", function_id))
        })?;
        let function = function.clone();
        drop(functions);

        let mut instances = self.instances.write().await;
        let function_instances = instances.entry(function_id.to_string()).or_default();

        let current = function_instances.len();

        if target_instances > current {
            // Scale up
            for _ in current..target_instances {
                let instance = FunctionInstance {
                    id: Uuid::new_v4().to_string(),
                    function_id: function.id.clone(),
                    state: InstanceState::Cold,
                    created_at: Utc::now(),
                    last_used: Utc::now(),
                    invocation_count: 0,
                };
                function_instances.push(instance);
            }
            info!(
                "Scaled up function {} from {} to {} instances",
                function.name, current, target_instances
            );
        } else if target_instances < current {
            // Scale down
            function_instances.truncate(target_instances);
            info!(
                "Scaled down function {} from {} to {} instances",
                function.name, current, target_instances
            );
        }

        Ok(())
    }
}

/// FaaS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaasConfig {
    /// Maximum instances per function
    pub max_instances_per_function: usize,
    /// Instance idle timeout in seconds
    pub instance_idle_timeout_seconds: u64,
    /// Default memory limit per function (MB)
    pub default_memory_mb: u32,
    /// Default timeout per invocation (seconds)
    pub default_timeout_seconds: u32,
    /// Enable auto-scaling
    pub auto_scaling_enabled: bool,
}

impl Default for FaasConfig {
    fn default() -> Self {
        Self {
            max_instances_per_function: 10,
            instance_idle_timeout_seconds: 300,
            default_memory_mb: 128,
            default_timeout_seconds: 30,
            auto_scaling_enabled: true,
        }
    }
}

/// Function specification for deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSpec {
    /// Function name
    pub name: String,
    /// Runtime
    pub runtime: Runtime,
    /// Function code (source or WASM binary base64)
    pub code: String,
    /// Handler function name
    pub handler: String,
    /// Memory limit in MB
    pub memory_mb: Option<u32>,
    /// Timeout in seconds
    pub timeout_seconds: Option<u32>,
    /// Environment variables
    pub environment: HashMap<String, String>,
    /// Additional configuration
    pub config: HashMap<String, serde_json::Value>,
}

impl FunctionSpec {
    /// Create a new function spec
    pub fn new(
        name: impl Into<String>,
        runtime: Runtime,
        code: impl Into<String>,
        handler: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            runtime,
            code: code.into(),
            handler: handler.into(),
            memory_mb: None,
            timeout_seconds: None,
            environment: HashMap::new(),
            config: HashMap::new(),
        }
    }

    /// Set memory limit
    pub fn with_memory(mut self, mb: u32) -> Self {
        self.memory_mb = Some(mb);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, seconds: u32) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Add environment variable
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }
}

/// Function runtime
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Runtime {
    /// Rust (compiled to WASM)
    Rust,
    /// JavaScript (QuickJS)
    JavaScript,
    /// Python (RustPython)
    Python,
    /// Go (TinyGo WASM)
    Go,
    /// Pre-compiled WASM
    Wasm,
}

impl std::fmt::Display for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Runtime::Rust => write!(f, "rust"),
            Runtime::JavaScript => write!(f, "javascript"),
            Runtime::Python => write!(f, "python"),
            Runtime::Go => write!(f, "go"),
            Runtime::Wasm => write!(f, "wasm"),
        }
    }
}

/// Deployed function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    /// Function ID
    pub id: String,
    /// Function name
    pub name: String,
    /// Runtime
    pub runtime: Runtime,
    /// Function code
    pub code: String,
    /// Handler
    pub handler: String,
    /// Memory limit (MB)
    pub memory_mb: u32,
    /// Timeout (seconds)
    pub timeout_seconds: u32,
    /// Environment variables
    pub environment: HashMap<String, String>,
    /// State
    pub state: FunctionState,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
    /// Version
    pub version: u32,
    /// Additional config
    pub config: HashMap<String, serde_json::Value>,
}

/// Function state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionState {
    /// Deployed and ready
    Deployed,
    /// Disabled
    Disabled,
    /// Error state
    Error,
}

/// Function summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSummary {
    /// Function ID
    pub id: String,
    /// Function name
    pub name: String,
    /// Runtime
    pub runtime: Runtime,
    /// State
    pub state: FunctionState,
    /// Version
    pub version: u32,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
}

/// Function instance
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FunctionInstance {
    id: String,
    function_id: String,
    state: InstanceState,
    created_at: DateTime<Utc>,
    last_used: DateTime<Utc>,
    invocation_count: u64,
}

/// Instance state
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
enum InstanceState {
    /// Cold (not initialized)
    Cold,
    /// Warm (ready)
    Warm,
    /// Busy (executing)
    Busy,
}

/// Trigger specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerSpec {
    /// Trigger type
    pub trigger_type: TriggerType,
    /// Configuration
    pub config: HashMap<String, serde_json::Value>,
}

impl TriggerSpec {
    /// Create a topic trigger
    pub fn topic(topic: impl Into<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("topic".to_string(), serde_json::json!(topic.into()));
        Self {
            trigger_type: TriggerType::Topic,
            config,
        }
    }

    /// Create an HTTP trigger
    pub fn http(path: impl Into<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("path".to_string(), serde_json::json!(path.into()));
        Self {
            trigger_type: TriggerType::Http,
            config,
        }
    }

    /// Create a schedule trigger
    pub fn schedule(cron: impl Into<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("cron".to_string(), serde_json::json!(cron.into()));
        Self {
            trigger_type: TriggerType::Schedule,
            config,
        }
    }
}

/// Trigger type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerType {
    /// Triggered by topic message
    Topic,
    /// Triggered by HTTP request
    Http,
    /// Triggered on schedule (cron)
    Schedule,
    /// Triggered by another function
    Chain,
}

/// Trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    /// Trigger ID
    pub id: String,
    /// Function ID
    pub function_id: String,
    /// Trigger type
    pub trigger_type: TriggerType,
    /// Configuration
    pub config: HashMap<String, serde_json::Value>,
    /// Enabled
    pub enabled: bool,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
}

/// Invocation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRequest {
    /// Request payload
    pub payload: serde_json::Value,
    /// Invocation context
    pub context: InvocationContext,
}

/// Invocation context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationContext {
    /// Trigger type
    pub trigger_type: TriggerType,
    /// Source (topic name, HTTP path, etc.)
    pub source: String,
}

/// Invocation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResult {
    /// Request ID
    pub request_id: String,
    /// Function ID
    pub function_id: String,
    /// Output
    pub output: serde_json::Value,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Status
    pub status: InvocationStatus,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Function logs
    pub logs: Vec<String>,
}

/// Invocation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationStatus {
    /// Success
    Success,
    /// Failed
    Failed,
    /// Timed out
    Timeout,
}

/// FaaS stats (internal)
#[derive(Default)]
struct FaasStats {
    functions_deployed: AtomicU64,
    functions_deleted: AtomicU64,
    invocations: AtomicU64,
    successful_invocations: AtomicU64,
    failed_invocations: AtomicU64,
    cold_starts: AtomicU64,
}

/// FaaS stats snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaasStatsSnapshot {
    /// Functions deployed
    pub functions_deployed: u64,
    /// Functions deleted
    pub functions_deleted: u64,
    /// Total invocations
    pub invocations: u64,
    /// Successful invocations
    pub successful_invocations: u64,
    /// Failed invocations
    pub failed_invocations: u64,
    /// Cold starts
    pub cold_starts: u64,
}

/// Simple glob matching
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                if pattern_chars.peek().is_none() {
                    return true;
                }
                while text_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                return false;
            }
            c => {
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_faas_creation() {
        let config = FaasConfig::default();
        let manager = FaasManager::new(config);
        assert!(manager.list_functions().await.is_empty());
    }

    #[tokio::test]
    async fn test_deploy_function() {
        let manager = FaasManager::new(FaasConfig::default());

        let spec = FunctionSpec::new(
            "hello",
            Runtime::JavaScript,
            "function handler(event) { return { message: 'Hello!' }; }",
            "handler",
        )
        .with_memory(128)
        .with_timeout(30);

        let function = manager.deploy_function(spec).await.unwrap();
        assert_eq!(function.name, "hello");
        assert_eq!(function.runtime, Runtime::JavaScript);
        assert_eq!(function.state, FunctionState::Deployed);
    }

    #[tokio::test]
    async fn test_invoke_function() {
        let manager = FaasManager::new(FaasConfig::default());

        let spec = FunctionSpec::new(
            "test-func",
            Runtime::JavaScript,
            "function handler(event) { return event; }",
            "handler",
        );

        let function = manager.deploy_function(spec).await.unwrap();

        let request = InvocationRequest {
            payload: serde_json::json!({"key": "value"}),
            context: InvocationContext {
                trigger_type: TriggerType::Http,
                source: "/test".to_string(),
            },
        };

        let result = manager.invoke(&function.id, request).await.unwrap();
        assert_eq!(result.status, InvocationStatus::Success);
    }

    #[tokio::test]
    async fn test_add_trigger() {
        let manager = FaasManager::new(FaasConfig::default());

        let spec = FunctionSpec::new("trigger-func", Runtime::JavaScript, "code", "handler");
        let function = manager.deploy_function(spec).await.unwrap();

        let trigger = manager
            .add_trigger(&function.id, TriggerSpec::topic("events"))
            .await
            .unwrap();

        assert_eq!(trigger.trigger_type, TriggerType::Topic);
        assert_eq!(trigger.function_id, function.id);

        let triggers = manager.list_triggers(&function.id).await;
        assert_eq!(triggers.len(), 1);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("events*", "events-clicks"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("prefix-*-suffix", "prefix-middle-suffix"));
        assert!(!glob_match("events*", "other"));
    }
}
