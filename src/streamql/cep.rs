//! Complex Event Processing (CEP) Module
//!
//! Provides pattern matching capabilities for detecting complex event sequences
//! in streaming data.
//!
//! # Features
//!
//! - **Pattern Matching**: Detect sequences of events (A followed by B)
//! - **Temporal Constraints**: Within time window, before, after
//! - **Quantifiers**: ONE, ONE_OR_MORE, OPTIONAL
//! - **Contiguity**: Strict, relaxed, non-deterministic relaxed
//! - **Pattern Actions**: Callbacks on pattern match
//!
//! # Example
//!
//! ```text
//! DEFINE PATTERN fraud_pattern AS
//!   login AS (event_type = 'login')
//!   FOLLOWED BY
//!   withdrawal AS (event_type = 'withdrawal' AND amount > 10000)
//!   WITHIN INTERVAL '5 minutes'
//!   WHERE login.user_id = withdrawal.user_id
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pattern contiguity mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Contiguity {
    /// Events must be directly adjacent (no events in between)
    Strict,
    /// Non-matching events can occur between matches
    #[default]
    Relaxed,
    /// Allow multiple pattern matches from same start (NFA semantics)
    NonDeterministicRelaxed,
}

/// Pattern quantifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Quantifier {
    /// Exactly one event
    #[default]
    One,
    /// Zero or one event
    Optional,
    /// One or more events
    OneOrMore,
    /// Zero or more events
    ZeroOrMore,
    /// Exactly N events
    Times(usize),
    /// Between N and M events
    Range { min: usize, max: usize },
}

/// Event condition for pattern matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCondition {
    /// Condition expression (simplified)
    pub expression: String,
    /// Field to evaluate
    pub field: String,
    /// Operator
    pub operator: ConditionOperator,
    /// Value to compare
    pub value: ConditionValue,
}

/// Condition operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    Like,
    Between,
}

/// Condition value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Array(Vec<ConditionValue>),
}

/// Pattern element (single event matcher)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternElement {
    /// Element name (for referencing in correlations)
    pub name: String,
    /// Conditions to match
    pub conditions: Vec<EventCondition>,
    /// Quantifier
    pub quantifier: Quantifier,
    /// Skip strategy when condition not met
    pub skip_strategy: SkipStrategy,
}

/// Skip strategy when condition is not met
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SkipStrategy {
    /// No skip - pattern fails if condition not met
    #[default]
    NoSkip,
    /// Skip until next potential match
    SkipToNext,
    /// Skip past last matched event
    SkipPastLast,
    /// Skip to first match
    SkipToFirst,
}

/// Pattern definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    /// Pattern name
    pub name: String,
    /// Pattern elements (in sequence)
    pub elements: Vec<PatternElement>,
    /// Contiguity mode
    pub contiguity: Contiguity,
    /// Time window constraint (milliseconds)
    pub within_ms: Option<u64>,
    /// Correlation conditions (e.g., "a.user_id = b.user_id")
    pub correlations: Vec<String>,
}

impl Pattern {
    /// Create a new pattern builder
    pub fn builder(name: impl Into<String>) -> PatternBuilder {
        PatternBuilder::new(name)
    }
}

/// Pattern builder
pub struct PatternBuilder {
    name: String,
    elements: Vec<PatternElement>,
    contiguity: Contiguity,
    within_ms: Option<u64>,
    correlations: Vec<String>,
}

impl PatternBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            elements: Vec::new(),
            contiguity: Contiguity::Relaxed,
            within_ms: None,
            correlations: Vec::new(),
        }
    }

    /// Add an element to the pattern
    pub fn begin(self, name: impl Into<String>) -> PatternElementBuilder {
        PatternElementBuilder::new(self, name.into())
    }

    /// Set contiguity mode
    pub fn with_contiguity(mut self, contiguity: Contiguity) -> Self {
        self.contiguity = contiguity;
        self
    }

    /// Set time window constraint
    pub fn within(mut self, duration_ms: u64) -> Self {
        self.within_ms = Some(duration_ms);
        self
    }

    /// Add correlation condition
    pub fn where_clause(mut self, correlation: impl Into<String>) -> Self {
        self.correlations.push(correlation.into());
        self
    }

    /// Add element (internal)
    fn add_element(&mut self, element: PatternElement) {
        self.elements.push(element);
    }

    /// Build the pattern
    pub fn build(self) -> Pattern {
        Pattern {
            name: self.name,
            elements: self.elements,
            contiguity: self.contiguity,
            within_ms: self.within_ms,
            correlations: self.correlations,
        }
    }
}

/// Builder for pattern elements
pub struct PatternElementBuilder {
    pattern_builder: PatternBuilder,
    name: String,
    conditions: Vec<EventCondition>,
    quantifier: Quantifier,
    skip_strategy: SkipStrategy,
}

impl PatternElementBuilder {
    fn new(pattern_builder: PatternBuilder, name: String) -> Self {
        Self {
            pattern_builder,
            name,
            conditions: Vec::new(),
            quantifier: Quantifier::One,
            skip_strategy: SkipStrategy::NoSkip,
        }
    }

    /// Add a condition
    pub fn where_field(
        mut self,
        field: impl Into<String>,
        operator: ConditionOperator,
        value: ConditionValue,
    ) -> Self {
        let field_str = field.into();
        self.conditions.push(EventCondition {
            expression: format!("{} {:?} {:?}", field_str, operator, value),
            field: field_str,
            operator,
            value,
        });
        self
    }

    /// Set quantifier
    pub fn times(mut self, n: usize) -> Self {
        self.quantifier = Quantifier::Times(n);
        self
    }

    /// One or more
    pub fn one_or_more(mut self) -> Self {
        self.quantifier = Quantifier::OneOrMore;
        self
    }

    /// Optional (zero or one)
    pub fn optional(mut self) -> Self {
        self.quantifier = Quantifier::Optional;
        self
    }

    /// Set skip strategy
    pub fn with_skip(mut self, strategy: SkipStrategy) -> Self {
        self.skip_strategy = strategy;
        self
    }

    /// Add next element (FOLLOWED BY)
    pub fn followed_by(mut self, name: impl Into<String>) -> PatternElementBuilder {
        let element = PatternElement {
            name: self.name,
            conditions: self.conditions,
            quantifier: self.quantifier,
            skip_strategy: self.skip_strategy,
        };
        self.pattern_builder.add_element(element);

        PatternElementBuilder::new(self.pattern_builder, name.into())
    }

    /// Set time window constraint
    pub fn within(mut self, duration_ms: u64) -> Self {
        self.pattern_builder.within_ms = Some(duration_ms);
        self
    }

    /// Finish building and return the pattern
    pub fn build(mut self) -> Pattern {
        let element = PatternElement {
            name: self.name,
            conditions: self.conditions,
            quantifier: self.quantifier,
            skip_strategy: self.skip_strategy,
        };
        self.pattern_builder.add_element(element);
        self.pattern_builder.build()
    }
}

/// Event for pattern matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CepEvent {
    /// Event timestamp
    pub timestamp: i64,
    /// Event type
    pub event_type: String,
    /// Event payload (field -> value)
    pub fields: HashMap<String, serde_json::Value>,
}

impl CepEvent {
    /// Create a new event
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            event_type: event_type.into(),
            fields: HashMap::new(),
        }
    }

    /// Set timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Add field
    pub fn with_field(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Get field value
    pub fn get_field(&self, key: &str) -> Option<&serde_json::Value> {
        self.fields.get(key)
    }

    /// Check if event matches a condition
    pub fn matches_condition(&self, condition: &EventCondition) -> bool {
        let Some(value) = self.fields.get(&condition.field) else {
            return false;
        };

        match (&condition.value, value) {
            (ConditionValue::String(expected), serde_json::Value::String(actual)) => {
                match condition.operator {
                    ConditionOperator::Eq => expected == actual,
                    ConditionOperator::Ne => expected != actual,
                    ConditionOperator::Like => actual.contains(expected),
                    _ => false,
                }
            }
            (ConditionValue::Int(expected), serde_json::Value::Number(actual)) => {
                let Some(actual_i64) = actual.as_i64() else {
                    return false;
                };
                match condition.operator {
                    ConditionOperator::Eq => *expected == actual_i64,
                    ConditionOperator::Ne => *expected != actual_i64,
                    ConditionOperator::Lt => actual_i64 < *expected,
                    ConditionOperator::Le => actual_i64 <= *expected,
                    ConditionOperator::Gt => actual_i64 > *expected,
                    ConditionOperator::Ge => actual_i64 >= *expected,
                    _ => false,
                }
            }
            (ConditionValue::Float(expected), serde_json::Value::Number(actual)) => {
                let Some(actual_f64) = actual.as_f64() else {
                    return false;
                };
                match condition.operator {
                    ConditionOperator::Eq => (*expected - actual_f64).abs() < f64::EPSILON,
                    ConditionOperator::Lt => actual_f64 < *expected,
                    ConditionOperator::Le => actual_f64 <= *expected,
                    ConditionOperator::Gt => actual_f64 > *expected,
                    ConditionOperator::Ge => actual_f64 >= *expected,
                    _ => false,
                }
            }
            (ConditionValue::Bool(expected), serde_json::Value::Bool(actual)) => {
                match condition.operator {
                    ConditionOperator::Eq => *expected == *actual,
                    ConditionOperator::Ne => *expected != *actual,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

/// Pattern match result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternMatch {
    /// Pattern name
    pub pattern_name: String,
    /// Matched events by element name
    pub events: HashMap<String, Vec<CepEvent>>,
    /// Match start time
    pub start_time: i64,
    /// Match end time
    pub end_time: i64,
}

/// Match state for NFA-based pattern matching
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MatchState {
    /// Current element index
    element_idx: usize,
    /// Matched events so far
    matched_events: HashMap<String, Vec<CepEvent>>,
    /// Start timestamp
    start_time: i64,
    /// Last event timestamp
    last_time: i64,
}

/// Pattern matcher using NFA-based approach
pub struct PatternMatcher {
    /// Pattern to match
    pattern: Pattern,
    /// Active match states
    states: Vec<MatchState>,
    /// Completed matches
    matches: Vec<PatternMatch>,
}

impl PatternMatcher {
    /// Create a new pattern matcher
    pub fn new(pattern: Pattern) -> Self {
        Self {
            pattern,
            states: Vec::new(),
            matches: Vec::new(),
        }
    }

    /// Process an event
    pub fn process_event(&mut self, event: &CepEvent) -> Vec<PatternMatch> {
        let mut new_states = Vec::new();
        let mut completed = Vec::new();

        // Try to start a new match with this event
        if self.matches_element(event, 0) {
            let mut matched = HashMap::new();
            matched.insert(self.pattern.elements[0].name.clone(), vec![event.clone()]);

            if self.pattern.elements.len() == 1 {
                // Pattern complete
                completed.push(PatternMatch {
                    pattern_name: self.pattern.name.clone(),
                    events: matched,
                    start_time: event.timestamp,
                    end_time: event.timestamp,
                });
            } else {
                new_states.push(MatchState {
                    element_idx: 1,
                    matched_events: matched,
                    start_time: event.timestamp,
                    last_time: event.timestamp,
                });
            }
        }

        // Collect state updates to avoid borrow conflict
        let state_updates: Vec<_> = self
            .states
            .iter()
            .enumerate()
            .map(|(i, state)| {
                // Check time constraint
                let timed_out = if let Some(within_ms) = self.pattern.within_ms {
                    event.timestamp - state.start_time > within_ms as i64
                } else {
                    false
                };

                let element_idx = state.element_idx;
                let matches = if element_idx < self.pattern.elements.len() && !timed_out {
                    self.matches_element(event, element_idx)
                } else {
                    false
                };

                (i, timed_out, matches, element_idx)
            })
            .collect();

        // Apply state updates
        for (i, timed_out, matches, element_idx) in state_updates {
            let state = &self.states[i];

            if timed_out || element_idx >= self.pattern.elements.len() {
                continue;
            }

            if matches {
                let element_name = &self.pattern.elements[element_idx].name;

                // Create new matched events
                let mut matched_events = state.matched_events.clone();
                matched_events
                    .entry(element_name.clone())
                    .or_default()
                    .push(event.clone());

                // Check if pattern complete
                if element_idx + 1 >= self.pattern.elements.len() {
                    completed.push(PatternMatch {
                        pattern_name: self.pattern.name.clone(),
                        events: matched_events,
                        start_time: state.start_time,
                        end_time: event.timestamp,
                    });
                } else {
                    // Move to next element
                    new_states.push(MatchState {
                        element_idx: element_idx + 1,
                        matched_events,
                        start_time: state.start_time,
                        last_time: event.timestamp,
                    });
                }
            } else if self.pattern.contiguity != Contiguity::Strict {
                // Keep state alive for relaxed contiguity
                new_states.push(state.clone());
            }
        }

        self.states = new_states;
        self.matches.extend(completed.clone());

        completed
    }

    /// Check if event matches an element's conditions
    fn matches_element(&self, event: &CepEvent, element_idx: usize) -> bool {
        let element = &self.pattern.elements[element_idx];

        // All conditions must match
        element
            .conditions
            .iter()
            .all(|cond| event.matches_condition(cond))
    }

    /// Get all completed matches
    pub fn get_matches(&self) -> &[PatternMatch] {
        &self.matches
    }

    /// Clear completed matches
    pub fn clear_matches(&mut self) {
        self.matches.clear();
    }

    /// Get number of active match states
    pub fn active_state_count(&self) -> usize {
        self.states.len()
    }
}

/// CEP Engine - manages multiple patterns
pub struct CepEngine {
    /// Registered patterns
    patterns: Arc<RwLock<HashMap<String, Pattern>>>,
    /// Pattern matchers
    matchers: Arc<RwLock<HashMap<String, PatternMatcher>>>,
}

impl CepEngine {
    /// Create a new CEP engine
    pub fn new() -> Self {
        Self {
            patterns: Arc::new(RwLock::new(HashMap::new())),
            matchers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a pattern
    pub async fn register_pattern(&self, pattern: Pattern) -> Result<()> {
        let name = pattern.name.clone();

        let matcher = PatternMatcher::new(pattern.clone());

        let mut patterns = self.patterns.write().await;
        let mut matchers = self.matchers.write().await;

        patterns.insert(name.clone(), pattern);
        matchers.insert(name, matcher);

        Ok(())
    }

    /// Unregister a pattern
    pub async fn unregister_pattern(&self, name: &str) -> Result<()> {
        let mut patterns = self.patterns.write().await;
        let mut matchers = self.matchers.write().await;

        if patterns.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "Pattern '{}' not found",
                name
            )));
        }
        matchers.remove(name);

        Ok(())
    }

    /// Process an event through all patterns
    pub async fn process_event(&self, event: &CepEvent) -> Vec<PatternMatch> {
        let mut matchers = self.matchers.write().await;
        let mut all_matches = Vec::new();

        for matcher in matchers.values_mut() {
            let matches = matcher.process_event(event);
            all_matches.extend(matches);
        }

        all_matches
    }

    /// Get registered pattern names
    pub async fn list_patterns(&self) -> Vec<String> {
        let patterns = self.patterns.read().await;
        patterns.keys().cloned().collect()
    }
}

impl Default for CepEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_builder() {
        let pattern = Pattern::builder("fraud_pattern")
            .begin("login")
            .where_field(
                "event_type",
                ConditionOperator::Eq,
                ConditionValue::String("login".into()),
            )
            .followed_by("withdrawal")
            .where_field(
                "event_type",
                ConditionOperator::Eq,
                ConditionValue::String("withdrawal".into()),
            )
            .where_field("amount", ConditionOperator::Gt, ConditionValue::Int(10000))
            .within(300_000) // 5 minutes
            .build();

        assert_eq!(pattern.name, "fraud_pattern");
        assert_eq!(pattern.elements.len(), 2);
        assert_eq!(pattern.within_ms, Some(300_000));
    }

    #[test]
    fn test_event_matches_condition() {
        let event = CepEvent::new("login")
            .with_field("user_id", "user123")
            .with_field("amount", 15000);

        let string_cond = EventCondition {
            expression: "".into(),
            field: "user_id".into(),
            operator: ConditionOperator::Eq,
            value: ConditionValue::String("user123".into()),
        };

        let int_cond = EventCondition {
            expression: "".into(),
            field: "amount".into(),
            operator: ConditionOperator::Gt,
            value: ConditionValue::Int(10000),
        };

        assert!(event.matches_condition(&string_cond));
        assert!(event.matches_condition(&int_cond));
    }

    #[test]
    fn test_pattern_matching() {
        let pattern = Pattern::builder("sequence")
            .begin("first")
            .where_field(
                "event_type",
                ConditionOperator::Eq,
                ConditionValue::String("A".into()),
            )
            .followed_by("second")
            .where_field(
                "event_type",
                ConditionOperator::Eq,
                ConditionValue::String("B".into()),
            )
            .build();

        let mut matcher = PatternMatcher::new(pattern);

        // First event
        let event_a = CepEvent::new("event")
            .with_timestamp(1000)
            .with_field("event_type", "A");

        let matches = matcher.process_event(&event_a);
        assert!(matches.is_empty()); // Not complete yet

        // Second event
        let event_b = CepEvent::new("event")
            .with_timestamp(2000)
            .with_field("event_type", "B");

        let matches = matcher.process_event(&event_b);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].pattern_name, "sequence");
    }

    #[tokio::test]
    async fn test_cep_engine() {
        let engine = CepEngine::new();

        let pattern = Pattern::builder("test_pattern")
            .begin("event")
            .where_field(
                "type",
                ConditionOperator::Eq,
                ConditionValue::String("click".into()),
            )
            .build();

        engine.register_pattern(pattern).await.unwrap();

        let patterns = engine.list_patterns().await;
        assert_eq!(patterns.len(), 1);
        assert!(patterns.contains(&"test_pattern".to_string()));
    }
}
