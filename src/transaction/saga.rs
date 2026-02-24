//! Saga orchestration pattern for distributed stream workflows.
//!
//! Provides a declarative way to orchestrate multi-step workflows across
//! topics with automatic compensation (rollback) on failure.
//!
//! # Example
//!
//! ```text
//! SagaBuilder::new("order-fulfillment")
//!     .step("reserve-inventory")
//!         .action_topic("inventory-commands", r#"{"cmd":"reserve"}"#)
//!         .compensate_topic("inventory-commands", r#"{"cmd":"release"}"#)
//!         .reply_topic("inventory-replies")
//!     .step("charge-payment")
//!         .action_topic("payment-commands", r#"{"cmd":"charge"}"#)
//!         .compensate_topic("payment-commands", r#"{"cmd":"refund"}"#)
//!         .reply_topic("payment-replies")
//!     .step("ship-order")
//!         .action_topic("shipping-commands", r#"{"cmd":"ship"}"#)
//!         .compensate_topic("shipping-commands", r#"{"cmd":"cancel"}"#)
//!         .reply_topic("shipping-replies")
//!     .build()
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Unique saga execution identifier.
pub type SagaId = String;

/// Saga definition — the blueprint for a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaDefinition {
    /// Saga name (unique identifier for the workflow type).
    pub name: String,
    /// Ordered list of steps.
    pub steps: Vec<SagaStep>,
    /// Global timeout in milliseconds for the entire saga.
    pub timeout_ms: u64,
    /// Maximum number of retry attempts per step.
    pub max_retries: u32,
}

/// A single step in a saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaStep {
    /// Step name (unique within the saga).
    pub name: String,
    /// Action: topic and message to produce for the forward operation.
    pub action: SagaAction,
    /// Compensation: topic and message to produce for rollback.
    pub compensation: Option<SagaAction>,
    /// Topic to listen for the reply.
    pub reply_topic: String,
    /// Timeout for this step in milliseconds.
    pub step_timeout_ms: u64,
}

/// An action to produce to a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaAction {
    /// Target topic.
    pub topic: String,
    /// Message template (may contain `{{saga_id}}`, `{{step}}` placeholders).
    pub message_template: String,
    /// Optional message key.
    pub key: Option<String>,
    /// Additional headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// State of a saga execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaState {
    /// Saga is running forward steps.
    Running,
    /// A step failed; compensating previous steps.
    Compensating,
    /// All steps completed successfully.
    Completed,
    /// Saga was compensated (rolled back) successfully.
    Compensated,
    /// Saga failed and compensation also failed.
    Failed,
    /// Saga timed out.
    TimedOut,
}

/// State of an individual step execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepState {
    /// Waiting to be executed.
    Pending,
    /// Action has been sent, awaiting reply.
    AwaitingReply,
    /// Step completed successfully.
    Completed,
    /// Step failed.
    Failed,
    /// Compensation sent.
    Compensating,
    /// Compensation completed.
    Compensated,
    /// Compensation failed.
    CompensationFailed,
}

/// A running saga instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaExecution {
    /// Unique execution ID.
    pub id: SagaId,
    /// Name of the saga definition.
    pub saga_name: String,
    /// Overall saga state.
    pub state: SagaState,
    /// Per-step execution state.
    pub step_states: Vec<StepExecution>,
    /// Index of the currently active step (forward or compensating).
    pub current_step: usize,
    /// When this execution started.
    pub started_at: DateTime<Utc>,
    /// When this execution completed (if finished).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if failed.
    pub error: Option<String>,
    /// Context data passed between steps.
    pub context: HashMap<String, serde_json::Value>,
    /// Number of retries so far for current step.
    pub retry_count: u32,
}

/// Per-step execution state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepExecution {
    /// Step name.
    pub step_name: String,
    /// Step state.
    pub state: StepState,
    /// Reply data (if received).
    pub reply: Option<serde_json::Value>,
    /// When the action was sent.
    pub action_sent_at: Option<DateTime<Utc>>,
    /// When the reply was received.
    pub reply_received_at: Option<DateTime<Utc>>,
}

/// Saga orchestrator — manages saga definitions and executions.
pub struct SagaOrchestrator {
    definitions: HashMap<String, SagaDefinition>,
    executions: HashMap<SagaId, SagaExecution>,
}

impl SagaOrchestrator {
    /// Create a new saga orchestrator.
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            executions: HashMap::new(),
        }
    }

    /// Register a saga definition.
    pub fn register(&mut self, definition: SagaDefinition) -> Result<()> {
        if definition.steps.is_empty() {
            return Err(StreamlineError::Config(
                "Saga must have at least one step".into(),
            ));
        }
        info!(saga = %definition.name, steps = definition.steps.len(), "Registered saga definition");
        self.definitions.insert(definition.name.clone(), definition);
        Ok(())
    }

    /// Start a new saga execution.
    pub fn start_saga(
        &mut self,
        saga_name: &str,
        context: HashMap<String, serde_json::Value>,
    ) -> Result<SagaStartResult> {
        let definition = self
            .definitions
            .get(saga_name)
            .ok_or_else(|| {
                StreamlineError::Config(format!("Saga definition '{}' not found", saga_name))
            })?
            .clone();

        let saga_id = uuid::Uuid::new_v4().to_string();
        let step_states: Vec<StepExecution> = definition
            .steps
            .iter()
            .map(|s| StepExecution {
                step_name: s.name.clone(),
                state: StepState::Pending,
                reply: None,
                action_sent_at: None,
                reply_received_at: None,
            })
            .collect();

        let execution = SagaExecution {
            id: saga_id.clone(),
            saga_name: saga_name.to_string(),
            state: SagaState::Running,
            step_states,
            current_step: 0,
            started_at: Utc::now(),
            completed_at: None,
            error: None,
            context,
            retry_count: 0,
        };

        // Determine the first action to produce
        let first_step = &definition.steps[0];
        let action_message = self.render_template(
            &first_step.action.message_template,
            &saga_id,
            &first_step.name,
        );

        self.executions.insert(saga_id.clone(), execution);

        info!(saga_id = %saga_id, saga = %saga_name, "Started saga execution");

        Ok(SagaStartResult {
            saga_id,
            first_action: ProduceAction {
                topic: first_step.action.topic.clone(),
                key: first_step.action.key.clone(),
                message: action_message,
                headers: first_step.action.headers.clone(),
            },
            reply_topic: first_step.reply_topic.clone(),
        })
    }

    /// Handle a reply for a saga step.
    ///
    /// Returns the next action to produce (either forward or compensation).
    pub fn handle_reply(
        &mut self,
        saga_id: &str,
        success: bool,
        reply_data: Option<serde_json::Value>,
    ) -> Result<SagaReplyResult> {
        let execution = self
            .executions
            .get_mut(saga_id)
            .ok_or_else(|| {
                StreamlineError::Config(format!("Saga execution '{}' not found", saga_id))
            })?;

        let definition = self
            .definitions
            .get(&execution.saga_name)
            .cloned()
            .ok_or_else(|| {
                StreamlineError::Config(format!(
                    "Saga definition '{}' not found",
                    execution.saga_name
                ))
            })?;

        let current = execution.current_step;

        match execution.state {
            SagaState::Running => {
                execution.step_states[current].reply = reply_data;
                execution.step_states[current].reply_received_at = Some(Utc::now());

                if success {
                    execution.step_states[current].state = StepState::Completed;
                    debug!(saga_id, step = current, "Saga step completed");

                    // Move to next step
                    if current + 1 >= definition.steps.len() {
                        // All steps done
                        execution.state = SagaState::Completed;
                        execution.completed_at = Some(Utc::now());
                        info!(saga_id, "Saga completed successfully");
                        return Ok(SagaReplyResult::Completed);
                    }

                    execution.current_step = current + 1;
                    execution.retry_count = 0;
                    let next_step = &definition.steps[execution.current_step];
                    execution.step_states[execution.current_step].state =
                        StepState::AwaitingReply;
                    execution.step_states[execution.current_step].action_sent_at =
                        Some(Utc::now());

                    let message = self.render_template(
                        &next_step.action.message_template,
                        saga_id,
                        &next_step.name,
                    );

                    Ok(SagaReplyResult::NextAction(ProduceAction {
                        topic: next_step.action.topic.clone(),
                        key: next_step.action.key.clone(),
                        message,
                        headers: next_step.action.headers.clone(),
                    }))
                } else {
                    // Step failed — begin compensation
                    execution.step_states[current].state = StepState::Failed;
                    execution.state = SagaState::Compensating;
                    execution.error = Some(format!("Step '{}' failed", definition.steps[current].name));
                    warn!(saga_id, step = current, "Saga step failed, starting compensation");

                    self.next_compensation(saga_id, &definition, current)
                }
            }
            SagaState::Compensating => {
                execution.step_states[current].state = if success {
                    StepState::Compensated
                } else {
                    StepState::CompensationFailed
                };

                if current == 0 {
                    execution.state = SagaState::Compensated;
                    execution.completed_at = Some(Utc::now());
                    info!(saga_id, "Saga fully compensated");
                    return Ok(SagaReplyResult::Compensated);
                }

                self.next_compensation(saga_id, &definition, current - 1)
            }
            _ => Ok(SagaReplyResult::AlreadyFinished),
        }
    }

    /// Find the next compensation step to execute.
    fn next_compensation(
        &mut self,
        saga_id: &str,
        definition: &SagaDefinition,
        from_step: usize,
    ) -> Result<SagaReplyResult> {
        // Walk backwards to find the previous completed step with a compensation
        for i in (0..=from_step).rev() {
            let execution = self.executions.get_mut(saga_id).ok_or_else(|| {
                StreamlineError::Config("Saga not found".into())
            })?;

            if execution.step_states[i].state == StepState::Completed {
                if let Some(ref comp) = definition.steps[i].compensation {
                    execution.current_step = i;
                    execution.step_states[i].state = StepState::Compensating;

                    let message =
                        self.render_template(&comp.message_template, saga_id, &definition.steps[i].name);

                    return Ok(SagaReplyResult::Compensate(ProduceAction {
                        topic: comp.topic.clone(),
                        key: comp.key.clone(),
                        message,
                        headers: comp.headers.clone(),
                    }));
                }
            }
        }

        // No more compensations needed
        if let Some(execution) = self.executions.get_mut(saga_id) {
            execution.state = SagaState::Compensated;
            execution.completed_at = Some(Utc::now());
        }
        Ok(SagaReplyResult::Compensated)
    }

    /// Get saga execution status.
    pub fn get_execution(&self, saga_id: &str) -> Option<&SagaExecution> {
        self.executions.get(saga_id)
    }

    /// List all active saga executions.
    pub fn list_active(&self) -> Vec<&SagaExecution> {
        self.executions
            .values()
            .filter(|e| e.state == SagaState::Running || e.state == SagaState::Compensating)
            .collect()
    }

    /// Render template placeholders.
    fn render_template(&self, template: &str, saga_id: &str, step_name: &str) -> String {
        template
            .replace("{{saga_id}}", saga_id)
            .replace("{{step}}", step_name)
            .replace("{{timestamp}}", &Utc::now().timestamp_millis().to_string())
    }
}

/// Result from starting a saga.
#[derive(Debug, Clone)]
pub struct SagaStartResult {
    pub saga_id: SagaId,
    pub first_action: ProduceAction,
    pub reply_topic: String,
}

/// An action to produce to a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceAction {
    pub topic: String,
    pub key: Option<String>,
    pub message: String,
    pub headers: HashMap<String, String>,
}

/// Result from handling a step reply.
#[derive(Debug)]
pub enum SagaReplyResult {
    /// Produce the next forward action.
    NextAction(ProduceAction),
    /// All steps completed.
    Completed,
    /// Produce a compensation action.
    Compensate(ProduceAction),
    /// All compensations completed.
    Compensated,
    /// Saga was already finished.
    AlreadyFinished,
}

/// Builder for saga definitions.
pub struct SagaBuilder {
    name: String,
    steps: Vec<SagaStep>,
    timeout_ms: u64,
    max_retries: u32,
}

impl SagaBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            timeout_ms: 300_000, // 5 minutes default
            max_retries: 3,
        }
    }

    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn step(self, name: impl Into<String>) -> SagaStepBuilder {
        SagaStepBuilder {
            saga_builder: self,
            name: name.into(),
            action: None,
            compensation: None,
            reply_topic: String::new(),
            step_timeout_ms: 30_000,
        }
    }

    pub fn build(self) -> SagaDefinition {
        SagaDefinition {
            name: self.name,
            steps: self.steps,
            timeout_ms: self.timeout_ms,
            max_retries: self.max_retries,
        }
    }
}

/// Builder for individual saga steps.
pub struct SagaStepBuilder {
    saga_builder: SagaBuilder,
    name: String,
    action: Option<SagaAction>,
    compensation: Option<SagaAction>,
    reply_topic: String,
    step_timeout_ms: u64,
}

impl SagaStepBuilder {
    pub fn action_topic(mut self, topic: impl Into<String>, message: impl Into<String>) -> Self {
        self.action = Some(SagaAction {
            topic: topic.into(),
            message_template: message.into(),
            key: None,
            headers: HashMap::new(),
        });
        self
    }

    pub fn compensate_topic(
        mut self,
        topic: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.compensation = Some(SagaAction {
            topic: topic.into(),
            message_template: message.into(),
            key: None,
            headers: HashMap::new(),
        });
        self
    }

    pub fn reply_topic(mut self, topic: impl Into<String>) -> Self {
        self.reply_topic = topic.into();
        self
    }

    pub fn step_timeout_ms(mut self, ms: u64) -> Self {
        self.step_timeout_ms = ms;
        self
    }

    pub fn step(mut self, name: impl Into<String>) -> SagaStepBuilder {
        self.saga_builder.steps.push(SagaStep {
            name: self.name,
            action: self.action.unwrap_or_else(|| SagaAction {
                topic: String::new(),
                message_template: String::new(),
                key: None,
                headers: HashMap::new(),
            }),
            compensation: self.compensation,
            reply_topic: self.reply_topic,
            step_timeout_ms: self.step_timeout_ms,
        });
        SagaStepBuilder {
            saga_builder: self.saga_builder,
            name: name.into(),
            action: None,
            compensation: None,
            reply_topic: String::new(),
            step_timeout_ms: 30_000,
        }
    }

    pub fn build(mut self) -> SagaDefinition {
        self.saga_builder.steps.push(SagaStep {
            name: self.name,
            action: self.action.unwrap_or_else(|| SagaAction {
                topic: String::new(),
                message_template: String::new(),
                key: None,
                headers: HashMap::new(),
            }),
            compensation: self.compensation,
            reply_topic: self.reply_topic,
            step_timeout_ms: self.step_timeout_ms,
        });
        self.saga_builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_definition() -> SagaDefinition {
        SagaBuilder::new("order-saga")
            .step("reserve")
            .action_topic("inventory-cmd", r#"{"cmd":"reserve","saga":"{{saga_id}}"}"#)
            .compensate_topic("inventory-cmd", r#"{"cmd":"release","saga":"{{saga_id}}"}"#)
            .reply_topic("inventory-reply")
            .step("charge")
            .action_topic("payment-cmd", r#"{"cmd":"charge","saga":"{{saga_id}}"}"#)
            .compensate_topic("payment-cmd", r#"{"cmd":"refund","saga":"{{saga_id}}"}"#)
            .reply_topic("payment-reply")
            .step("ship")
            .action_topic("shipping-cmd", r#"{"cmd":"ship","saga":"{{saga_id}}"}"#)
            .reply_topic("shipping-reply")
            .build()
    }

    #[test]
    fn test_saga_builder() {
        let def = test_definition();
        assert_eq!(def.name, "order-saga");
        assert_eq!(def.steps.len(), 3);
        assert_eq!(def.steps[0].name, "reserve");
        assert!(def.steps[0].compensation.is_some());
        assert!(def.steps[2].compensation.is_none());
    }

    #[test]
    fn test_saga_happy_path() {
        let mut orch = SagaOrchestrator::new();
        orch.register(test_definition()).unwrap();

        // Start saga
        let start = orch
            .start_saga("order-saga", HashMap::new())
            .unwrap();
        assert_eq!(start.first_action.topic, "inventory-cmd");

        // Step 1 success
        let result = orch
            .handle_reply(&start.saga_id, true, None)
            .unwrap();
        match result {
            SagaReplyResult::NextAction(action) => {
                assert_eq!(action.topic, "payment-cmd");
            }
            other => panic!("Expected NextAction, got {:?}", other),
        }

        // Step 2 success
        let result = orch
            .handle_reply(&start.saga_id, true, None)
            .unwrap();
        match result {
            SagaReplyResult::NextAction(action) => {
                assert_eq!(action.topic, "shipping-cmd");
            }
            other => panic!("Expected NextAction, got {:?}", other),
        }

        // Step 3 success — saga complete
        let result = orch
            .handle_reply(&start.saga_id, true, None)
            .unwrap();
        assert!(matches!(result, SagaReplyResult::Completed));

        let exec = orch.get_execution(&start.saga_id).unwrap();
        assert_eq!(exec.state, SagaState::Completed);
    }

    #[test]
    fn test_saga_compensation() {
        let mut orch = SagaOrchestrator::new();
        orch.register(test_definition()).unwrap();

        let start = orch
            .start_saga("order-saga", HashMap::new())
            .unwrap();

        // Step 1 success
        orch.handle_reply(&start.saga_id, true, None).unwrap();
        // Step 2 fails — should trigger compensation
        let result = orch
            .handle_reply(&start.saga_id, false, None)
            .unwrap();

        match result {
            SagaReplyResult::Compensate(action) => {
                // Should compensate step 1 (reserve)
                assert_eq!(action.topic, "inventory-cmd");
                assert!(action.message.contains("release"));
            }
            other => panic!("Expected Compensate, got {:?}", other),
        }

        // Compensation reply
        let result = orch
            .handle_reply(&start.saga_id, true, None)
            .unwrap();
        assert!(matches!(result, SagaReplyResult::Compensated));

        let exec = orch.get_execution(&start.saga_id).unwrap();
        assert_eq!(exec.state, SagaState::Compensated);
    }

    #[test]
    fn test_template_rendering() {
        let orch = SagaOrchestrator::new();
        let rendered = orch.render_template(
            r#"{"saga":"{{saga_id}}","step":"{{step}}"}"#,
            "test-id",
            "reserve",
        );
        assert!(rendered.contains("test-id"));
        assert!(rendered.contains("reserve"));
    }

    #[test]
    fn test_list_active() {
        let mut orch = SagaOrchestrator::new();
        orch.register(test_definition()).unwrap();

        let start = orch
            .start_saga("order-saga", HashMap::new())
            .unwrap();
        assert_eq!(orch.list_active().len(), 1);

        orch.handle_reply(&start.saga_id, true, None).unwrap();
        orch.handle_reply(&start.saga_id, true, None).unwrap();
        orch.handle_reply(&start.saga_id, true, None).unwrap();
        assert_eq!(orch.list_active().len(), 0);
    }
}
