//! Trigger bindings — map topics to functions.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP method for trigger matching.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

/// Trigger type — what causes a function to execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerType {
    /// Trigger on messages arriving in a topic.
    Topic {
        /// Source topic name.
        topic: String,
        /// Optional partition filter (None = all partitions).
        partitions: Option<Vec<i32>>,
        /// Consumer group ID for offset management.
        group_id: String,
    },
    /// Trigger on a cron schedule.
    Schedule {
        /// Cron expression (e.g., "0 */5 * * * *" for every 5 min).
        cron: String,
    },
    /// Trigger via HTTP endpoint.
    Http {
        /// URL path (e.g., "/fn/my-function").
        path: String,
        /// Allowed HTTP methods.
        methods: Vec<HttpMethod>,
    },
    /// Trigger on a named event pattern.
    Event {
        /// Event pattern to match (supports simple glob-style matching).
        pattern: String,
    },
}

/// Trigger configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Whether this trigger is enabled.
    pub enabled: bool,
    /// Batch size: how many messages to send per invocation.
    pub batch_size: usize,
    /// Maximum wait time for batch to fill (milliseconds).
    pub batch_timeout_ms: u64,
    /// Starting offset policy for topic triggers.
    pub offset_policy: OffsetPolicy,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 1,
            batch_timeout_ms: 100,
            offset_policy: OffsetPolicy::Latest,
        }
    }
}

/// Where to start consuming from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OffsetPolicy {
    /// Start from the latest offset.
    Latest,
    /// Start from the earliest offset.
    Earliest,
    /// Start from a specific offset.
    Specific(i64),
}

/// A binding between a trigger and a function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerBinding {
    /// Unique binding name.
    pub name: String,
    /// Function name to invoke.
    pub function_name: String,
    /// Trigger type and source.
    pub trigger_type: TriggerType,
    /// Trigger configuration.
    pub config: TriggerConfig,
    /// Filter expression: only invoke on matching events.
    pub filter: Option<String>,
    /// Additional metadata.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl TriggerBinding {
    /// Create a new topic trigger binding.
    pub fn topic(
        name: impl Into<String>,
        function_name: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        let fn_name = function_name.into();
        Self {
            name: name.into(),
            function_name: fn_name.clone(),
            trigger_type: TriggerType::Topic {
                topic: topic.into(),
                partitions: None,
                group_id: format!("faas-{fn_name}"),
            },
            config: TriggerConfig::default(),
            filter: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new scheduled trigger binding.
    pub fn schedule(
        name: impl Into<String>,
        function_name: impl Into<String>,
        cron: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            function_name: function_name.into(),
            trigger_type: TriggerType::Schedule { cron: cron.into() },
            config: TriggerConfig::default(),
            filter: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new event trigger binding.
    pub fn event(
        name: impl Into<String>,
        function_name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            function_name: function_name.into(),
            trigger_type: TriggerType::Event {
                pattern: pattern.into(),
            },
            config: TriggerConfig::default(),
            filter: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new HTTP trigger binding with typed methods.
    pub fn http(
        name: impl Into<String>,
        function_name: impl Into<String>,
        path: impl Into<String>,
        methods: Vec<HttpMethod>,
    ) -> Self {
        Self {
            name: name.into(),
            function_name: function_name.into(),
            trigger_type: TriggerType::Http {
                path: path.into(),
                methods,
            },
            config: TriggerConfig::default(),
            filter: None,
            metadata: HashMap::new(),
        }
    }

    /// Set batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set filter expression.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

/// Batch configuration for trigger message accumulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum number of messages in a batch.
    pub max_batch_size: u32,
    /// Maximum time to wait for a batch to fill (milliseconds).
    pub max_wait_ms: u64,
    /// Optional byte-size limit for a batch.
    pub byte_limit: Option<u64>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_wait_ms: 1000,
            byte_limit: None,
        }
    }
}

/// Manages trigger bindings with efficient lookup indices.
pub struct TriggerManager {
    bindings: HashMap<String, TriggerBinding>,
    topic_index: HashMap<String, Vec<String>>,
    http_index: HashMap<String, Vec<String>>,
    schedule_index: Vec<String>,
    event_index: Vec<String>,
}

impl Default for TriggerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TriggerManager {
    /// Create a new empty trigger manager.
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
            topic_index: HashMap::new(),
            http_index: HashMap::new(),
            schedule_index: Vec::new(),
            event_index: Vec::new(),
        }
    }

    /// Register a trigger binding and update indices.
    pub fn register(&mut self, binding: TriggerBinding) -> Result<()> {
        if self.bindings.contains_key(&binding.name) {
            return Err(StreamlineError::Config(format!(
                "Trigger binding '{}' already exists",
                binding.name
            )));
        }

        let name = binding.name.clone();
        match &binding.trigger_type {
            TriggerType::Topic { topic, .. } => {
                self.topic_index
                    .entry(topic.clone())
                    .or_default()
                    .push(name.clone());
            }
            TriggerType::Schedule { .. } => {
                self.schedule_index.push(name.clone());
            }
            TriggerType::Http { path, .. } => {
                self.http_index
                    .entry(path.clone())
                    .or_default()
                    .push(name.clone());
            }
            TriggerType::Event { .. } => {
                self.event_index.push(name.clone());
            }
        }

        self.bindings.insert(name, binding);
        Ok(())
    }

    /// Unregister a trigger binding and clean up indices.
    pub fn unregister(&mut self, name: &str) -> Result<()> {
        let binding = self
            .bindings
            .remove(name)
            .ok_or_else(|| StreamlineError::Config(format!("Binding '{name}' not found")))?;

        match &binding.trigger_type {
            TriggerType::Topic { topic, .. } => {
                if let Some(names) = self.topic_index.get_mut(topic) {
                    names.retain(|n| n != name);
                    if names.is_empty() {
                        self.topic_index.remove(topic);
                    }
                }
            }
            TriggerType::Schedule { .. } => {
                self.schedule_index.retain(|n| n != name);
            }
            TriggerType::Http { path, .. } => {
                if let Some(names) = self.http_index.get_mut(path) {
                    names.retain(|n| n != name);
                    if names.is_empty() {
                        self.http_index.remove(path);
                    }
                }
            }
            TriggerType::Event { .. } => {
                self.event_index.retain(|n| n != name);
            }
        }

        Ok(())
    }

    /// Find all bindings triggered by a given topic name.
    pub fn match_topic(&self, topic: &str) -> Vec<&TriggerBinding> {
        self.topic_index
            .get(topic)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|n| self.bindings.get(n))
                    .filter(|b| b.config.enabled)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find all HTTP bindings matching a path and method.
    pub fn match_http(&self, path: &str, method: &HttpMethod) -> Vec<&TriggerBinding> {
        self.http_index
            .get(path)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|n| self.bindings.get(n))
                    .filter(|b| {
                        b.config.enabled
                            && match &b.trigger_type {
                                TriggerType::Http { methods, .. } => methods.contains(method),
                                _ => false,
                            }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find all event bindings whose pattern matches the given event name.
    pub fn match_event(&self, event: &str) -> Vec<&TriggerBinding> {
        self.event_index
            .iter()
            .filter_map(|n| self.bindings.get(n))
            .filter(|b| {
                b.config.enabled
                    && match &b.trigger_type {
                        TriggerType::Event { pattern } => {
                            if pattern.ends_with('*') {
                                let prefix = &pattern[..pattern.len() - 1];
                                event.starts_with(prefix)
                            } else {
                                event == pattern
                            }
                        }
                        _ => false,
                    }
            })
            .collect()
    }

    /// Get all schedule-based bindings.
    pub fn scheduled_bindings(&self) -> Vec<&TriggerBinding> {
        self.schedule_index
            .iter()
            .filter_map(|n| self.bindings.get(n))
            .filter(|b| b.config.enabled)
            .collect()
    }

    /// Get all registered bindings.
    pub fn all_bindings(&self) -> Vec<&TriggerBinding> {
        self.bindings.values().collect()
    }

    /// Number of registered bindings.
    pub fn binding_count(&self) -> usize {
        self.bindings.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_trigger() {
        let binding = TriggerBinding::topic("my-trigger", "process-fn", "events");
        assert_eq!(binding.name, "my-trigger");
        assert_eq!(binding.function_name, "process-fn");
        match &binding.trigger_type {
            TriggerType::Topic {
                topic, group_id, ..
            } => {
                assert_eq!(topic, "events");
                assert_eq!(group_id, "faas-process-fn");
            }
            _ => panic!("Expected topic trigger"),
        }
    }

    #[test]
    fn test_schedule_trigger() {
        let binding = TriggerBinding::schedule("cron-trigger", "cleanup-fn", "0 0 * * *");
        match &binding.trigger_type {
            TriggerType::Schedule { cron } => assert_eq!(cron, "0 0 * * *"),
            _ => panic!("Expected schedule trigger"),
        }
    }

    #[test]
    fn test_trigger_builder() {
        let binding = TriggerBinding::topic("t", "fn", "topic")
            .with_batch_size(100)
            .with_filter("level = 'error'");
        assert_eq!(binding.config.batch_size, 100);
        assert_eq!(binding.filter, Some("level = 'error'".to_string()));
    }

    #[test]
    fn test_event_trigger_construction() {
        let binding = TriggerBinding::event("evt-trigger", "handler-fn", "user.created");
        assert_eq!(binding.name, "evt-trigger");
        assert_eq!(binding.function_name, "handler-fn");
        match &binding.trigger_type {
            TriggerType::Event { pattern } => assert_eq!(pattern, "user.created"),
            _ => panic!("Expected event trigger"),
        }
    }

    #[test]
    fn test_http_trigger_construction() {
        let binding = TriggerBinding::http(
            "api-trigger",
            "api-fn",
            "/api/v1/data",
            vec![HttpMethod::Get, HttpMethod::Post],
        );
        assert_eq!(binding.name, "api-trigger");
        match &binding.trigger_type {
            TriggerType::Http { path, methods } => {
                assert_eq!(path, "/api/v1/data");
                assert_eq!(methods.len(), 2);
                assert!(methods.contains(&HttpMethod::Get));
                assert!(methods.contains(&HttpMethod::Post));
            }
            _ => panic!("Expected HTTP trigger"),
        }
    }

    #[test]
    fn test_batch_config_defaults() {
        let config = BatchConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_wait_ms, 1000);
        assert!(config.byte_limit.is_none());
    }

    #[test]
    fn test_trigger_manager_register_unregister() {
        let mut mgr = TriggerManager::new();
        assert_eq!(mgr.binding_count(), 0);
        mgr.register(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        assert_eq!(mgr.binding_count(), 1);
        mgr.unregister("t1").unwrap();
        assert_eq!(mgr.binding_count(), 0);
    }

    #[test]
    fn test_trigger_manager_duplicate_rejected() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        assert!(mgr
            .register(TriggerBinding::topic("t1", "fn-b", "other"))
            .is_err());
    }

    #[test]
    fn test_trigger_manager_match_topic() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        mgr.register(TriggerBinding::topic("t2", "fn-b", "events"))
            .unwrap();
        mgr.register(TriggerBinding::topic("t3", "fn-c", "other"))
            .unwrap();
        assert_eq!(mgr.match_topic("events").len(), 2);
        assert_eq!(mgr.match_topic("other").len(), 1);
        assert_eq!(mgr.match_topic("nonexistent").len(), 0);
    }

    #[test]
    fn test_trigger_manager_match_http() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::http(
            "h1",
            "fn-a",
            "/api/data",
            vec![HttpMethod::Get, HttpMethod::Post],
        ))
        .unwrap();
        mgr.register(TriggerBinding::http(
            "h2",
            "fn-b",
            "/api/data",
            vec![HttpMethod::Delete],
        ))
        .unwrap();
        let matches = mgr.match_http("/api/data", &HttpMethod::Get);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "h1");
        assert_eq!(mgr.match_http("/api/data", &HttpMethod::Delete).len(), 1);
        assert_eq!(mgr.match_http("/api/data", &HttpMethod::Patch).len(), 0);
        assert_eq!(mgr.match_http("/api/other", &HttpMethod::Get).len(), 0);
    }

    #[test]
    fn test_trigger_manager_match_event() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::event("e1", "fn-a", "user.*"))
            .unwrap();
        mgr.register(TriggerBinding::event("e2", "fn-b", "order.created"))
            .unwrap();
        assert_eq!(mgr.match_event("user.created").len(), 1);
        assert_eq!(mgr.match_event("user.deleted").len(), 1);
        let matches = mgr.match_event("order.created");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "e2");
        assert_eq!(mgr.match_event("order.deleted").len(), 0);
    }

    #[test]
    fn test_trigger_manager_scheduled_bindings() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::schedule("s1", "fn-a", "0 * * * *"))
            .unwrap();
        mgr.register(TriggerBinding::topic("t1", "fn-b", "events"))
            .unwrap();
        let scheduled = mgr.scheduled_bindings();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].name, "s1");
    }

    #[test]
    fn test_trigger_manager_all_bindings() {
        let mut mgr = TriggerManager::new();
        mgr.register(TriggerBinding::topic("t1", "fn-a", "events"))
            .unwrap();
        mgr.register(TriggerBinding::schedule("s1", "fn-b", "0 * * * *"))
            .unwrap();
        mgr.register(TriggerBinding::event("e1", "fn-c", "user.*"))
            .unwrap();
        assert_eq!(mgr.all_bindings().len(), 3);
    }
}
