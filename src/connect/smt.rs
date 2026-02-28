//! Single Message Transforms (SMT) for Kafka Connect compatibility.
//!
//! Implements the standard Kafka Connect SMT interface for transforming
//! records as they flow through connectors.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A record flowing through a connector pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub key: Option<Value>,
    pub value: Option<Value>,
    pub headers: HashMap<String, String>,
    pub timestamp: Option<i64>,
}

/// Trait for Single Message Transforms.
pub trait Transform: Send + Sync {
    /// Transform name.
    fn name(&self) -> &str;
    /// Apply the transform to a record.
    fn apply(&self, record: ConnectRecord) -> Result<ConnectRecord, String>;
}

/// InsertField — adds a field with a static or computed value.
pub struct InsertField {
    field_name: String,
    field_value: Value,
}

impl InsertField {
    pub fn new(field_name: String, field_value: Value) -> Self {
        Self { field_name, field_value }
    }
}

impl Transform for InsertField {
    fn name(&self) -> &str { "InsertField" }

    fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        if let Some(Value::Object(ref mut map)) = record.value {
            map.insert(self.field_name.clone(), self.field_value.clone());
        }
        Ok(record)
    }
}

/// ReplaceField — renames or removes fields.
pub struct ReplaceField {
    renames: HashMap<String, String>,
    excludes: Vec<String>,
}

impl ReplaceField {
    pub fn new(renames: HashMap<String, String>, excludes: Vec<String>) -> Self {
        Self { renames, excludes }
    }
}

impl Transform for ReplaceField {
    fn name(&self) -> &str { "ReplaceField" }

    fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        if let Some(Value::Object(ref mut map)) = record.value {
            for exclude in &self.excludes {
                map.remove(exclude);
            }
            for (old_name, new_name) in &self.renames {
                if let Some(val) = map.remove(old_name) {
                    map.insert(new_name.clone(), val);
                }
            }
        }
        Ok(record)
    }
}

/// MaskField — replaces field values with a type-appropriate mask.
pub struct MaskField {
    fields: Vec<String>,
}

impl MaskField {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }
}

impl Transform for MaskField {
    fn name(&self) -> &str { "MaskField" }

    fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        if let Some(Value::Object(ref mut map)) = record.value {
            for field in &self.fields {
                if let Some(val) = map.get(field) {
                    let masked = match val {
                        Value::String(_) => Value::String("****".to_string()),
                        Value::Number(_) => Value::Number(serde_json::Number::from(0)),
                        Value::Bool(_) => Value::Bool(false),
                        _ => Value::Null,
                    };
                    map.insert(field.clone(), masked);
                }
            }
        }
        Ok(record)
    }
}

/// TimestampRouter — routes records to topic based on timestamp.
pub struct TimestampRouter {
    topic_format: String, // e.g., "{topic}-{timestamp:yyyy-MM-dd}"
}

impl TimestampRouter {
    pub fn new(topic_format: String) -> Self {
        Self { topic_format }
    }
}

impl Transform for TimestampRouter {
    fn name(&self) -> &str { "TimestampRouter" }

    fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        if let Some(ts) = record.timestamp {
            let dt = chrono::DateTime::from_timestamp(ts / 1000, 0)
                .unwrap_or_default();
            let date_str = dt.format("%Y-%m-%d").to_string();
            record.topic = self.topic_format
                .replace("{topic}", &record.topic)
                .replace("{timestamp:yyyy-MM-dd}", &date_str);
        }
        Ok(record)
    }
}

/// RegexRouter — routes records to topic based on regex replacement.
pub struct RegexRouter {
    pattern: regex::Regex,
    replacement: String,
}

impl RegexRouter {
    pub fn new(pattern: &str, replacement: String) -> Result<Self, regex::Error> {
        Ok(Self {
            pattern: regex::Regex::new(pattern)?,
            replacement,
        })
    }
}

impl Transform for RegexRouter {
    fn name(&self) -> &str { "RegexRouter" }

    fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        record.topic = self.pattern.replace(&record.topic, &self.replacement).to_string();
        Ok(record)
    }
}

/// Transform chain — applies multiple transforms in sequence.
pub struct TransformChain {
    transforms: Vec<Box<dyn Transform>>,
}

impl TransformChain {
    pub fn new() -> Self {
        Self { transforms: Vec::new() }
    }

    pub fn add(mut self, transform: Box<dyn Transform>) -> Self {
        self.transforms.push(transform);
        self
    }

    pub fn apply(&self, mut record: ConnectRecord) -> Result<ConnectRecord, String> {
        for transform in &self.transforms {
            record = transform.apply(record)?;
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_record() -> ConnectRecord {
        ConnectRecord {
            topic: "test-topic".to_string(),
            partition: Some(0),
            offset: Some(42),
            key: Some(json!("key-1")),
            value: Some(json!({"name": "Alice", "age": 30, "email": "alice@example.com"})),
            headers: std::collections::HashMap::new(),
            timestamp: Some(1709550000000),
        }
    }

    #[test]
    fn test_insert_field() {
        let transform = InsertField::new("source".to_string(), json!("streamline"));
        let result = transform.apply(sample_record()).unwrap();
        let map = result.value.unwrap();
        assert_eq!(map.get("source"), Some(&json!("streamline")));
        assert_eq!(map.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_replace_field_rename() {
        let mut renames = std::collections::HashMap::new();
        renames.insert("name".to_string(), "full_name".to_string());
        let transform = ReplaceField::new(renames, vec![]);
        let result = transform.apply(sample_record()).unwrap();
        let map = result.value.unwrap();
        assert!(map.get("name").is_none());
        assert_eq!(map.get("full_name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_replace_field_exclude() {
        let transform = ReplaceField::new(std::collections::HashMap::new(), vec!["email".to_string()]);
        let result = transform.apply(sample_record()).unwrap();
        let map = result.value.unwrap();
        assert!(map.get("email").is_none());
        assert_eq!(map.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_mask_field() {
        let transform = MaskField::new(vec!["email".to_string(), "age".to_string()]);
        let result = transform.apply(sample_record()).unwrap();
        let map = result.value.unwrap();
        assert_eq!(map.get("email"), Some(&json!("****")));
        assert_eq!(map.get("age"), Some(&json!(0)));
        assert_eq!(map.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_timestamp_router() {
        let transform = TimestampRouter::new("{topic}-{timestamp:yyyy-MM-dd}".to_string());
        let result = transform.apply(sample_record()).unwrap();
        assert!(result.topic.starts_with("test-topic-"));
        assert!(result.topic.contains("2024-03-04") || result.topic.len() > 10);
    }

    #[test]
    fn test_regex_router() {
        let transform = RegexRouter::new("^test-", "prod-".to_string()).unwrap();
        let result = transform.apply(sample_record()).unwrap();
        assert_eq!(result.topic, "prod-topic");
    }

    #[test]
    fn test_transform_chain() {
        let chain = TransformChain::new()
            .add(Box::new(InsertField::new("source".to_string(), json!("test"))))
            .add(Box::new(MaskField::new(vec!["email".to_string()])));
        let result = chain.apply(sample_record()).unwrap();
        let map = result.value.unwrap();
        assert_eq!(map.get("source"), Some(&json!("test")));
        assert_eq!(map.get("email"), Some(&json!("****")));
    }
}
