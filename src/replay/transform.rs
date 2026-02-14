//! Message transformation/mutation for replay

use crate::error::{Result, StreamlineError};
use crate::replay::storage::RecordedMessage;
use serde::{Deserialize, Serialize};

/// A rule for mutating messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutatorRule {
    /// Rule name
    pub name: String,
    /// Condition for when to apply (JSONPath expression or "all")
    #[serde(default = "default_condition")]
    pub condition: String,
    /// Type of mutation
    pub mutation: MutationType,
    /// Whether this rule is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_condition() -> String {
    "all".to_string()
}

fn default_enabled() -> bool {
    true
}

/// Types of mutations that can be applied
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MutationType {
    /// Replace a field value
    ReplaceField {
        /// JSONPath to the field
        path: String,
        /// New value
        value: serde_json::Value,
    },
    /// Set a field (create if not exists)
    SetField {
        /// JSONPath to the field
        path: String,
        /// Value to set
        value: serde_json::Value,
    },
    /// Delete a field
    DeleteField {
        /// JSONPath to the field
        path: String,
    },
    /// Rename a field
    RenameField {
        /// Original field name
        from: String,
        /// New field name
        to: String,
    },
    /// Add a header
    AddHeader {
        /// Header key
        key: String,
        /// Header value
        value: String,
    },
    /// Remove a header
    RemoveHeader {
        /// Header key to remove
        key: String,
    },
    /// Mask sensitive data
    MaskField {
        /// JSONPath to the field
        path: String,
        /// Mask character
        #[serde(default = "default_mask_char")]
        mask_char: char,
        /// Number of characters to show at start
        #[serde(default)]
        show_start: usize,
        /// Number of characters to show at end
        #[serde(default)]
        show_end: usize,
    },
    /// Change the topic
    ChangeTopic {
        /// New topic name
        topic: String,
    },
    /// Change the partition
    ChangePartition {
        /// New partition
        partition: i32,
    },
    /// Modify timestamp
    ModifyTimestamp {
        /// Offset to add (positive or negative)
        offset_ms: i64,
    },
    /// Replace the entire value
    ReplaceValue {
        /// New value (JSON or base64)
        value: String,
        /// Whether value is base64 encoded
        #[serde(default)]
        base64: bool,
    },
    /// Apply a template to the value
    Template {
        /// Template string with {{field}} placeholders
        template: String,
    },
    /// Filter/drop the message
    Drop,
}

fn default_mask_char() -> char {
    '*'
}

impl MutatorRule {
    /// Create a new mutator rule
    pub fn new(name: impl Into<String>, mutation: MutationType) -> Self {
        Self {
            name: name.into(),
            condition: default_condition(),
            mutation,
            enabled: true,
        }
    }

    /// Set the condition
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        self.condition = condition.into();
        self
    }

    /// Disable the rule
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Check if the rule should apply to a message
    fn matches(&self, msg: &RecordedMessage) -> bool {
        if !self.enabled {
            return false;
        }

        if self.condition == "all" {
            return true;
        }

        // Simple topic matching
        if let Some(topic) = self.condition.strip_prefix("topic:") {
            return msg.topic == topic || topic == "*";
        }

        // Simple partition matching
        if let Some(partition_str) = self.condition.strip_prefix("partition:") {
            if let Ok(partition) = partition_str.parse::<i32>()
            {
                return msg.partition == partition;
            }
        }

        true
    }
}

/// A single message mutator
pub struct MessageMutator {
    rule: MutatorRule,
}

impl MessageMutator {
    /// Create a new mutator from a rule
    pub fn new(rule: MutatorRule) -> Self {
        Self { rule }
    }

    /// Apply the mutation to a message
    pub fn apply(&self, mut msg: RecordedMessage) -> Result<Option<RecordedMessage>> {
        if !self.rule.matches(&msg) {
            return Ok(Some(msg));
        }

        match &self.rule.mutation {
            MutationType::Drop => {
                return Ok(None);
            }

            MutationType::ChangeTopic { topic } => {
                msg.topic = topic.clone();
            }

            MutationType::ChangePartition { partition } => {
                msg.partition = *partition;
            }

            MutationType::ModifyTimestamp { offset_ms } => {
                msg.timestamp += offset_ms;
            }

            MutationType::AddHeader { key, value } => {
                msg.headers.push(crate::replay::storage::RecordedHeader {
                    key: key.clone(),
                    value: base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        value.as_bytes(),
                    ),
                });
            }

            MutationType::RemoveHeader { key } => {
                msg.headers.retain(|h| h.key != *key);
            }

            MutationType::ReplaceValue {
                value,
                base64: is_base64,
            } => {
                msg.value = if *is_base64 {
                    value.clone()
                } else {
                    base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        value.as_bytes(),
                    )
                };
            }

            MutationType::ReplaceField { path, value } | MutationType::SetField { path, value } => {
                msg = self.apply_json_mutation(msg, |json| {
                    Self::set_field_at_path(json, path, value.clone())
                })?;
            }

            MutationType::DeleteField { path } => {
                msg =
                    self.apply_json_mutation(msg, |json| Self::delete_field_at_path(json, path))?;
            }

            MutationType::RenameField { from, to } => {
                msg = self.apply_json_mutation(msg, |json| {
                    if let serde_json::Value::Object(ref mut map) = json {
                        if let Some(value) = map.remove(from) {
                            map.insert(to.clone(), value);
                        }
                    }
                    Ok(())
                })?;
            }

            MutationType::MaskField {
                path,
                mask_char,
                show_start,
                show_end,
            } => {
                let mask = *mask_char;
                let start = *show_start;
                let end = *show_end;

                msg = self.apply_json_mutation(msg, |json| {
                    Self::mask_field_at_path(json, path, mask, start, end)
                })?;
            }

            MutationType::Template { template } => {
                // Parse existing value and apply template
                let value_bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &msg.value)
                        .map_err(|e| {
                            StreamlineError::storage_msg(format!("Invalid base64: {}", e))
                        })?;

                let value_str = String::from_utf8_lossy(&value_bytes);
                let json: serde_json::Value = serde_json::from_str(&value_str).unwrap_or_default();

                let mut result = template.clone();
                if let serde_json::Value::Object(map) = json {
                    for (key, val) in map {
                        let placeholder = format!("{{{{{}}}}}", key);
                        let replacement = match val {
                            serde_json::Value::String(s) => s,
                            _ => val.to_string(),
                        };
                        result = result.replace(&placeholder, &replacement);
                    }
                }

                msg.value = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    result.as_bytes(),
                );
            }
        }

        Ok(Some(msg))
    }

    /// Apply a mutation to the JSON value
    fn apply_json_mutation<F>(&self, mut msg: RecordedMessage, f: F) -> Result<RecordedMessage>
    where
        F: FnOnce(&mut serde_json::Value) -> Result<()>,
    {
        // Decode value
        let value_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &msg.value)
                .map_err(|e| StreamlineError::storage_msg(format!("Invalid base64: {}", e)))?;

        let value_str = String::from_utf8_lossy(&value_bytes);
        let mut json: serde_json::Value = serde_json::from_str(&value_str)
            .map_err(|e| StreamlineError::storage_msg(format!("Invalid JSON: {}", e)))?;

        // Apply mutation
        f(&mut json)?;

        // Re-encode
        let new_value = serde_json::to_string(&json)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to serialize: {}", e)))?;

        msg.value = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            new_value.as_bytes(),
        );

        Ok(msg)
    }

    /// Set a field at a simple path (dot notation)
    fn set_field_at_path(
        json: &mut serde_json::Value,
        path: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part - set the value
                if let serde_json::Value::Object(ref mut map) = current {
                    map.insert(part.to_string(), value);
                }
                return Ok(());
            }

            // Navigate to next level
            current = match current {
                serde_json::Value::Object(map) => {
                    if !map.contains_key(*part) {
                        map.insert(
                            part.to_string(),
                            serde_json::Value::Object(Default::default()),
                        );
                    }
                    map.get_mut(*part).ok_or_else(|| {
                        StreamlineError::storage_msg(format!(
                            "Cannot navigate path: {}",
                            path
                        ))
                    })?
                }
                _ => {
                    return Err(StreamlineError::storage_msg(format!(
                        "Cannot navigate path: {}",
                        path
                    )))
                }
            };
        }

        Ok(())
    }

    /// Delete a field at a simple path
    fn delete_field_at_path(json: &mut serde_json::Value, path: &str) -> Result<()> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                if let serde_json::Value::Object(ref mut map) = current {
                    map.remove(*part);
                }
                return Ok(());
            }

            current = match current {
                serde_json::Value::Object(map) => map.get_mut(*part).ok_or_else(|| {
                    StreamlineError::storage_msg(format!("Path not found: {}", path))
                })?,
                _ => {
                    return Err(StreamlineError::storage_msg(format!(
                        "Cannot navigate path: {}",
                        path
                    )))
                }
            };
        }

        Ok(())
    }

    /// Mask a string field
    fn mask_field_at_path(
        json: &mut serde_json::Value,
        path: &str,
        mask: char,
        show_start: usize,
        show_end: usize,
    ) -> Result<()> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                if let serde_json::Value::Object(ref mut map) = current {
                    if let Some(serde_json::Value::String(s)) = map.get(*part) {
                        let len = s.len();
                        let masked = if show_start + show_end >= len {
                            mask.to_string().repeat(len)
                        } else {
                            let start = &s[..show_start];
                            let end = &s[len - show_end..];
                            let middle = mask.to_string().repeat(len - show_start - show_end);
                            format!("{}{}{}", start, middle, end)
                        };
                        map.insert(part.to_string(), serde_json::Value::String(masked));
                    }
                }
                return Ok(());
            }

            current = match current {
                serde_json::Value::Object(map) => map.get_mut(*part).ok_or_else(|| {
                    StreamlineError::storage_msg(format!("Path not found: {}", path))
                })?,
                _ => {
                    return Err(StreamlineError::storage_msg(format!(
                        "Cannot navigate path: {}",
                        path
                    )))
                }
            };
        }

        Ok(())
    }
}

/// A chain of mutators to apply in sequence
pub struct MutatorChain {
    mutators: Vec<MessageMutator>,
}

impl MutatorChain {
    /// Create a new empty chain
    pub fn new() -> Self {
        Self {
            mutators: Vec::new(),
        }
    }

    /// Create a chain from rules
    pub fn from_rules(rules: Vec<MutatorRule>) -> Self {
        Self {
            mutators: rules.into_iter().map(MessageMutator::new).collect(),
        }
    }

    /// Add a mutator
    pub fn add(&mut self, rule: MutatorRule) {
        self.mutators.push(MessageMutator::new(rule));
    }

    /// Apply all mutations in sequence
    pub fn apply(&self, msg: RecordedMessage) -> Result<RecordedMessage> {
        let mut current = Some(msg);

        for mutator in &self.mutators {
            if let Some(m) = current {
                current = mutator.apply(m)?;
            } else {
                break;
            }
        }

        current
            .ok_or_else(|| StreamlineError::storage_msg("Message dropped by mutator".to_string()))
    }
}

impl Default for MutatorChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Record;
    use bytes::Bytes;

    fn create_test_message(value: &str) -> RecordedMessage {
        let record = Record::new(0, 1234567890, None, Bytes::from(value.to_string()));
        RecordedMessage::from_record(&record, "test-topic", 0)
    }

    #[test]
    fn test_change_topic() {
        let rule = MutatorRule::new(
            "change-topic",
            MutationType::ChangeTopic {
                topic: "new-topic".to_string(),
            },
        );
        let mutator = MessageMutator::new(rule);

        let msg = create_test_message(r#"{"key":"value"}"#);
        let result = mutator.apply(msg).unwrap().unwrap();

        assert_eq!(result.topic, "new-topic");
    }

    #[test]
    fn test_modify_timestamp() {
        let rule = MutatorRule::new(
            "shift-time",
            MutationType::ModifyTimestamp { offset_ms: 1000 },
        );
        let mutator = MessageMutator::new(rule);

        let mut msg = create_test_message(r#"{"key":"value"}"#);
        msg.timestamp = 5000;

        let result = mutator.apply(msg).unwrap().unwrap();
        assert_eq!(result.timestamp, 6000);
    }

    #[test]
    fn test_add_header() {
        let rule = MutatorRule::new(
            "add-header",
            MutationType::AddHeader {
                key: "x-replay".to_string(),
                value: "true".to_string(),
            },
        );
        let mutator = MessageMutator::new(rule);

        let msg = create_test_message(r#"{"key":"value"}"#);
        let result = mutator.apply(msg).unwrap().unwrap();

        assert_eq!(result.headers.len(), 1);
        assert_eq!(result.headers[0].key, "x-replay");
    }

    #[test]
    fn test_drop_message() {
        let rule = MutatorRule::new("drop", MutationType::Drop);
        let mutator = MessageMutator::new(rule);

        let msg = create_test_message(r#"{"key":"value"}"#);
        let result = mutator.apply(msg).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_set_field() {
        let rule = MutatorRule::new(
            "set-field",
            MutationType::SetField {
                path: "newField".to_string(),
                value: serde_json::json!("newValue"),
            },
        );
        let mutator = MessageMutator::new(rule);

        let msg = create_test_message(r#"{"key":"value"}"#);
        let result = mutator.apply(msg).unwrap().unwrap();

        let value_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &result.value)
                .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&value_bytes).unwrap();

        assert_eq!(json["newField"], "newValue");
    }

    #[test]
    fn test_mutator_chain() {
        let mut chain = MutatorChain::new();
        chain.add(MutatorRule::new(
            "change-topic",
            MutationType::ChangeTopic {
                topic: "modified".to_string(),
            },
        ));
        chain.add(MutatorRule::new(
            "shift-time",
            MutationType::ModifyTimestamp { offset_ms: 1000 },
        ));

        let mut msg = create_test_message(r#"{"key":"value"}"#);
        msg.timestamp = 5000;

        let result = chain.apply(msg).unwrap();

        assert_eq!(result.topic, "modified");
        assert_eq!(result.timestamp, 6000);
    }

    #[test]
    fn test_conditional_mutation() {
        let rule = MutatorRule::new(
            "change-topic",
            MutationType::ChangeTopic {
                topic: "matched".to_string(),
            },
        )
        .with_condition("topic:test-topic");

        let mutator = MessageMutator::new(rule);

        // Should match
        let msg1 = create_test_message(r#"{"key":"value"}"#);
        let result1 = mutator.apply(msg1).unwrap().unwrap();
        assert_eq!(result1.topic, "matched");

        // Create a message with different topic
        let mut msg2 = create_test_message(r#"{"key":"value"}"#);
        msg2.topic = "other-topic".to_string();
        let result2 = mutator.apply(msg2).unwrap().unwrap();
        assert_eq!(result2.topic, "other-topic"); // Not matched
    }
}
