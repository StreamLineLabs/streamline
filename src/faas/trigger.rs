//! Trigger bindings — map topics to functions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
        methods: Vec<String>,
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
                group_id: format!("faas-{}", fn_name),
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
            trigger_type: TriggerType::Schedule {
                cron: cron.into(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_trigger() {
        let binding = TriggerBinding::topic("my-trigger", "process-fn", "events");
        assert_eq!(binding.name, "my-trigger");
        assert_eq!(binding.function_name, "process-fn");
        match &binding.trigger_type {
            TriggerType::Topic { topic, group_id, .. } => {
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
}
