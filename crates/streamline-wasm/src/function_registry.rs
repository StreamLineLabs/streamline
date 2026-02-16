//! WASM Function Registry
//!
//! Manages deployed WebAssembly functions for stream processing.
//! Functions can be applied as transforms on topics, creating
//! serverless stream processing without external infrastructure.

use crate::error::{Result, WasmError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// A registered WASM function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmFunction {
    /// Function name (unique identifier)
    pub name: String,
    /// Description of what this function does
    pub description: String,
    /// WASM module bytes (compiled)
    #[serde(skip)]
    pub module_bytes: Vec<u8>,
    /// Input topic(s)
    pub input_topics: Vec<String>,
    /// Output topic (None = transform in-place)
    pub output_topic: Option<String>,
    /// Function type
    pub function_type: FunctionType,
    /// Current status
    pub status: FunctionStatus,
    /// Configuration key-value pairs
    pub config: HashMap<String, String>,
    /// Metrics
    #[serde(default)]
    pub metrics: FunctionMetrics,
    /// Created at
    pub created_at: DateTime<Utc>,
    /// Updated at
    pub updated_at: DateTime<Utc>,
}

/// The type of a WASM function
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionType {
    /// Transform: map input records to output records (1:1)
    Transform,
    /// Filter: decide whether to keep or drop records
    Filter,
    /// Aggregate: combine multiple records into one
    Aggregate,
    /// FlatMap: one input record to zero or more output records
    FlatMap,
}

impl std::fmt::Display for FunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionType::Transform => write!(f, "transform"),
            FunctionType::Filter => write!(f, "filter"),
            FunctionType::Aggregate => write!(f, "aggregate"),
            FunctionType::FlatMap => write!(f, "flat_map"),
        }
    }
}

/// Current status of a deployed function
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "reason")]
pub enum FunctionStatus {
    /// Function is being deployed
    Deploying,
    /// Function is actively processing records
    Running,
    /// Function is paused (not processing but still deployed)
    Paused,
    /// Function encountered an error
    Failed(String),
    /// Function has been stopped
    Stopped,
}

impl std::fmt::Display for FunctionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionStatus::Deploying => write!(f, "deploying"),
            FunctionStatus::Running => write!(f, "running"),
            FunctionStatus::Paused => write!(f, "paused"),
            FunctionStatus::Failed(reason) => write!(f, "failed: {}", reason),
            FunctionStatus::Stopped => write!(f, "stopped"),
        }
    }
}

/// Runtime metrics for a function
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FunctionMetrics {
    /// Total records processed
    pub records_processed: u64,
    /// Total records output
    pub records_output: u64,
    /// Total errors encountered
    pub errors: u64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// Timestamp of last processed record
    pub last_processed_at: Option<DateTime<Utc>>,
}

/// Registry managing deployed WASM functions
pub struct FunctionRegistry {
    functions: Arc<RwLock<HashMap<String, WasmFunction>>>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    /// Create a new empty function registry
    pub fn new() -> Self {
        Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Deploy (register) a new function
    pub async fn deploy(&self, function: WasmFunction) -> Result<()> {
        let name = function.name.clone();
        let mut functions = self.functions.write().await;
        if functions.contains_key(&name) {
            return Err(WasmError::Validation(format!(
                "Function '{}' already exists",
                name
            )));
        }
        functions.insert(name.clone(), function);
        info!(function = %name, "WASM function deployed");
        Ok(())
    }

    /// Get a function by name
    pub async fn get(&self, name: &str) -> Option<WasmFunction> {
        self.functions.read().await.get(name).cloned()
    }

    /// List all registered functions
    pub async fn list(&self) -> Vec<WasmFunction> {
        self.functions.read().await.values().cloned().collect()
    }

    /// Remove a function by name
    pub async fn remove(&self, name: &str) -> Result<()> {
        let mut functions = self.functions.write().await;
        functions
            .remove(name)
            .ok_or_else(|| WasmError::Internal(format!("Function '{}' not found", name)))?;
        info!(function = %name, "WASM function removed");
        Ok(())
    }

    /// Pause a running function
    pub async fn pause(&self, name: &str) -> Result<()> {
        let mut functions = self.functions.write().await;
        let function = functions
            .get_mut(name)
            .ok_or_else(|| WasmError::Internal(format!("Function '{}' not found", name)))?;

        match &function.status {
            FunctionStatus::Running => {
                function.status = FunctionStatus::Paused;
                function.updated_at = Utc::now();
                info!(function = %name, "WASM function paused");
                Ok(())
            }
            other => Err(WasmError::Validation(format!(
                "Cannot pause function in '{}' state",
                other
            ))),
        }
    }

    /// Resume a paused function
    pub async fn resume(&self, name: &str) -> Result<()> {
        let mut functions = self.functions.write().await;
        let function = functions
            .get_mut(name)
            .ok_or_else(|| WasmError::Internal(format!("Function '{}' not found", name)))?;

        match &function.status {
            FunctionStatus::Paused => {
                function.status = FunctionStatus::Running;
                function.updated_at = Utc::now();
                info!(function = %name, "WASM function resumed");
                Ok(())
            }
            other => Err(WasmError::Validation(format!(
                "Cannot resume function in '{}' state",
                other
            ))),
        }
    }

    /// Update metrics for a function after processing
    pub async fn update_metrics(
        &self,
        name: &str,
        records_in: u64,
        records_out: u64,
        errors: u64,
        latency_us: f64,
    ) {
        let mut functions = self.functions.write().await;
        if let Some(function) = functions.get_mut(name) {
            function.metrics.records_processed += records_in;
            function.metrics.records_output += records_out;
            function.metrics.errors += errors;
            // Rolling average
            let total = function.metrics.records_processed;
            if total > 0 {
                function.metrics.avg_latency_us = ((function.metrics.avg_latency_us
                    * (total - records_in) as f64)
                    + latency_us * records_in as f64)
                    / total as f64;
            }
            function.metrics.last_processed_at = Some(Utc::now());
        }
    }

    /// Get functions registered for a specific input topic
    pub async fn get_for_topic(&self, topic: &str) -> Vec<WasmFunction> {
        self.functions
            .read()
            .await
            .values()
            .filter(|f| {
                f.status == FunctionStatus::Running && f.input_topics.contains(&topic.to_string())
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_function(name: &str) -> WasmFunction {
        WasmFunction {
            name: name.to_string(),
            description: "Test function".to_string(),
            module_bytes: vec![0x00, 0x61, 0x73, 0x6d],
            input_topics: vec!["input-topic".to_string()],
            output_topic: Some("output-topic".to_string()),
            function_type: FunctionType::Transform,
            status: FunctionStatus::Running,
            config: HashMap::new(),
            metrics: FunctionMetrics::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_deploy_and_get() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-1")).await.unwrap();

        let func = registry.get("fn-1").await.unwrap();
        assert_eq!(func.name, "fn-1");
        assert_eq!(func.function_type, FunctionType::Transform);
    }

    #[tokio::test]
    async fn test_deploy_duplicate_fails() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-dup")).await.unwrap();
        assert!(registry.deploy(test_function("fn-dup")).await.is_err());
    }

    #[tokio::test]
    async fn test_list_functions() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-a")).await.unwrap();
        registry.deploy(test_function("fn-b")).await.unwrap();

        let fns = registry.list().await;
        assert_eq!(fns.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_function() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-rm")).await.unwrap();
        registry.remove("fn-rm").await.unwrap();
        assert!(registry.get("fn-rm").await.is_none());
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-pr")).await.unwrap();

        registry.pause("fn-pr").await.unwrap();
        let func = registry.get("fn-pr").await.unwrap();
        assert_eq!(func.status, FunctionStatus::Paused);

        registry.resume("fn-pr").await.unwrap();
        let func = registry.get("fn-pr").await.unwrap();
        assert_eq!(func.status, FunctionStatus::Running);
    }

    #[tokio::test]
    async fn test_get_for_topic() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-topic")).await.unwrap();

        let fns = registry.get_for_topic("input-topic").await;
        assert_eq!(fns.len(), 1);

        let fns = registry.get_for_topic("other-topic").await;
        assert!(fns.is_empty());
    }

    #[tokio::test]
    async fn test_update_metrics() {
        let registry = FunctionRegistry::new();
        registry.deploy(test_function("fn-metrics")).await.unwrap();

        registry
            .update_metrics("fn-metrics", 10, 10, 0, 50.0)
            .await;

        let func = registry.get("fn-metrics").await.unwrap();
        assert_eq!(func.metrics.records_processed, 10);
        assert_eq!(func.metrics.records_output, 10);
        assert!(func.metrics.last_processed_at.is_some());
    }

    #[test]
    fn test_function_type_display() {
        assert_eq!(FunctionType::Transform.to_string(), "transform");
        assert_eq!(FunctionType::Filter.to_string(), "filter");
        assert_eq!(FunctionType::Aggregate.to_string(), "aggregate");
        assert_eq!(FunctionType::FlatMap.to_string(), "flat_map");
    }

    #[test]
    fn test_function_status_display() {
        assert_eq!(FunctionStatus::Running.to_string(), "running");
        assert_eq!(FunctionStatus::Paused.to_string(), "paused");
        assert_eq!(FunctionStatus::Deploying.to_string(), "deploying");
        assert_eq!(FunctionStatus::Stopped.to_string(), "stopped");
        assert_eq!(
            FunctionStatus::Failed("oops".into()).to_string(),
            "failed: oops"
        );
    }
}
