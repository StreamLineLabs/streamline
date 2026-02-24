//! Function registry — stores and manages function deployments.

use super::function::{FaasFunction, FunctionConfig, FunctionMetrics, FunctionState};
use super::trigger::TriggerBinding;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Function registry — manages deployed functions and their bindings.
pub struct FunctionRegistry {
    /// Deployed functions by name.
    functions: HashMap<String, FaasFunction>,
    /// Trigger bindings by name.
    bindings: HashMap<String, TriggerBinding>,
    /// Function → bindings index.
    function_bindings: HashMap<String, Vec<String>>,
}

impl FunctionRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            bindings: HashMap::new(),
            function_bindings: HashMap::new(),
        }
    }

    /// Deploy a new function.
    pub fn deploy(&mut self, config: FunctionConfig) -> Result<()> {
        if self.functions.contains_key(&config.name) {
            return Err(StreamlineError::Config(format!(
                "Function '{}' is already deployed",
                config.name
            )));
        }

        info!(function = %config.name, "Deploying function");
        let func = FaasFunction::new(config.clone());
        self.functions.insert(config.name.clone(), func);
        self.function_bindings
            .insert(config.name.clone(), Vec::new());
        Ok(())
    }

    /// Undeploy a function and remove its bindings.
    pub fn undeploy(&mut self, name: &str) -> Result<()> {
        if !self.functions.contains_key(name) {
            return Err(StreamlineError::Config(format!(
                "Function '{}' not found",
                name
            )));
        }

        // Remove associated bindings
        if let Some(binding_names) = self.function_bindings.remove(name) {
            for b_name in &binding_names {
                self.bindings.remove(b_name);
            }
            debug!(
                function = name,
                bindings_removed = binding_names.len(),
                "Removed function bindings"
            );
        }

        self.functions.remove(name);
        info!(function = name, "Undeployed function");
        Ok(())
    }

    /// Add a trigger binding.
    pub fn add_binding(&mut self, binding: TriggerBinding) -> Result<()> {
        if !self.functions.contains_key(&binding.function_name) {
            return Err(StreamlineError::Config(format!(
                "Function '{}' not found; deploy it before adding bindings",
                binding.function_name
            )));
        }

        if self.bindings.contains_key(&binding.name) {
            return Err(StreamlineError::Config(format!(
                "Binding '{}' already exists",
                binding.name
            )));
        }

        let fn_name = binding.function_name.clone();
        let binding_name = binding.name.clone();

        self.bindings.insert(binding_name.clone(), binding);
        self.function_bindings
            .entry(fn_name)
            .or_default()
            .push(binding_name);

        Ok(())
    }

    /// Remove a trigger binding.
    pub fn remove_binding(&mut self, name: &str) -> Result<()> {
        let binding = self
            .bindings
            .remove(name)
            .ok_or_else(|| StreamlineError::Config(format!("Binding '{}' not found", name)))?;

        if let Some(bindings) = self.function_bindings.get_mut(&binding.function_name) {
            bindings.retain(|b| b != name);
        }

        Ok(())
    }

    /// Get a function by name.
    pub fn get_function(&self, name: &str) -> Option<&FaasFunction> {
        self.functions.get(name)
    }

    /// Get a mutable function by name.
    pub fn get_function_mut(&mut self, name: &str) -> Option<&mut FaasFunction> {
        self.functions.get_mut(name)
    }

    /// List all deployed functions.
    pub fn list_functions(&self) -> Vec<FunctionInfo> {
        self.functions
            .values()
            .map(|f| FunctionInfo {
                name: f.config.name.clone(),
                description: f.config.description.clone(),
                state: f.state,
                deployed_at: f.deployed_at.to_rfc3339(),
                metrics: f.metrics(),
                bindings: self
                    .function_bindings
                    .get(&f.config.name)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// List all bindings for a function.
    pub fn get_bindings_for(&self, function_name: &str) -> Vec<&TriggerBinding> {
        self.function_bindings
            .get(function_name)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|n| self.bindings.get(n))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all topic-triggered bindings (for the engine's consumer loop).
    pub fn topic_bindings(&self) -> Vec<&TriggerBinding> {
        self.bindings
            .values()
            .filter(|b| {
                matches!(b.trigger_type, super::trigger::TriggerType::Topic { .. })
                    && b.config.enabled
            })
            .collect()
    }

    /// Count deployed functions.
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }

    /// Count active bindings.
    pub fn binding_count(&self) -> usize {
        self.bindings.len()
    }
}

/// Summary info for a deployed function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub name: String,
    pub description: String,
    pub state: FunctionState,
    pub deployed_at: String,
    pub metrics: FunctionMetrics,
    pub bindings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faas::function::WasmSource;
    use crate::faas::trigger::TriggerBinding;

    fn test_config(name: &str) -> FunctionConfig {
        FunctionConfig {
            name: name.to_string(),
            description: format!("Test function {}", name),
            wasm_source: WasmSource::Bytes(vec![0, 97, 115, 109]),
            entry_point: "process".to_string(),
            env_vars: HashMap::new(),
            limits: Default::default(),
            output_topic: None,
            dlq_topic: None,
            max_retries: 3,
            tags: HashMap::new(),
        }
    }

    #[test]
    fn test_deploy_and_list() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.deploy(test_config("fn-b")).unwrap();

        assert_eq!(reg.function_count(), 2);
        let fns = reg.list_functions();
        assert_eq!(fns.len(), 2);
    }

    #[test]
    fn test_duplicate_deploy_rejected() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        assert!(reg.deploy(test_config("fn-a")).is_err());
    }

    #[test]
    fn test_undeploy() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.undeploy("fn-a").unwrap();
        assert_eq!(reg.function_count(), 0);
    }

    #[test]
    fn test_undeploy_removes_bindings() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.add_binding(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        assert_eq!(reg.binding_count(), 1);

        reg.undeploy("fn-a").unwrap();
        assert_eq!(reg.binding_count(), 0);
    }

    #[test]
    fn test_add_binding() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.add_binding(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();

        let bindings = reg.get_bindings_for("fn-a");
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_binding_requires_function() {
        let mut reg = FunctionRegistry::new();
        assert!(reg
            .add_binding(TriggerBinding::topic("t1", "nonexistent", "events"))
            .is_err());
    }

    #[test]
    fn test_remove_binding() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.add_binding(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        reg.remove_binding("t1").unwrap();
        assert_eq!(reg.binding_count(), 0);
    }

    #[test]
    fn test_topic_bindings() {
        let mut reg = FunctionRegistry::new();
        reg.deploy(test_config("fn-a")).unwrap();
        reg.add_binding(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        reg.add_binding(TriggerBinding::schedule("s1", "fn-a", "0 * * * *"))
            .unwrap();

        let topic_bindings = reg.topic_bindings();
        assert_eq!(topic_bindings.len(), 1);
    }
}
