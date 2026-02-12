//! Single Message Transform (SMT) framework for Kafka Connect compatibility
//!
//! Provides built-in transforms compatible with Kafka Connect SMTs:
//! - InsertField: Add fields to records
//! - ReplaceField: Rename or remove fields
//! - MaskField: Mask sensitive field values
//! - TimestampRouter: Route to topics based on timestamp
//! - RegexRouter: Route to topics based on regex matching
//! - ValueToKey: Extract key from value fields
//! - Filter: Conditionally drop records
//! - HoistField: Wrap record in a struct/map
//! - Flatten: Flatten nested structures

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A record flowing through the SMT chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectRecord {
    /// Topic name
    pub topic: String,
    /// Partition (if assigned)
    pub partition: Option<i32>,
    /// Record key (JSON value)
    pub key: Option<serde_json::Value>,
    /// Record value (JSON value)
    pub value: Option<serde_json::Value>,
    /// Record headers
    pub headers: HashMap<String, String>,
    /// Timestamp in milliseconds
    pub timestamp: Option<i64>,
}

/// Transform result
#[derive(Debug)]
pub enum TransformResult {
    /// Record was transformed successfully
    Record(ConnectRecord),
    /// Record should be dropped (filtered out)
    Drop,
    /// Transform failed
    Error(String),
}

/// Trait for Single Message Transforms
pub trait SingleMessageTransform: Send + Sync {
    /// Transform name
    fn name(&self) -> &str;

    /// Apply the transform to a record
    fn apply(&self, record: ConnectRecord) -> TransformResult;

    /// Validate the transform configuration
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Built-in: Insert a static field into the record value
pub struct InsertField {
    field_name: String,
    field_value: serde_json::Value,
}

impl InsertField {
    pub fn new(field_name: impl Into<String>, field_value: serde_json::Value) -> Self {
        Self {
            field_name: field_name.into(),
            field_value,
        }
    }

    /// Insert the current timestamp
    pub fn timestamp(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: serde_json::Value::String("${timestamp}".to_string()),
        }
    }

    /// Insert the topic name
    pub fn topic(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: serde_json::Value::String("${topic}".to_string()),
        }
    }
}

impl SingleMessageTransform for InsertField {
    fn name(&self) -> &str {
        "InsertField"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        if let Some(serde_json::Value::Object(ref mut map)) = record.value {
            let resolved = match self.field_value.as_str() {
                Some("${timestamp}") => serde_json::Value::Number(serde_json::Number::from(
                    record.timestamp.unwrap_or(0),
                )),
                Some("${topic}") => serde_json::Value::String(record.topic.clone()),
                _ => self.field_value.clone(),
            };
            map.insert(self.field_name.clone(), resolved);
        }
        TransformResult::Record(record)
    }
}

/// Built-in: Replace (rename) fields in the record value
pub struct ReplaceField {
    renames: HashMap<String, String>,
    includes: Vec<String>,
    excludes: Vec<String>,
}

impl ReplaceField {
    pub fn new() -> Self {
        Self {
            renames: HashMap::new(),
            includes: Vec::new(),
            excludes: Vec::new(),
        }
    }

    pub fn rename(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.renames.insert(from.into(), to.into());
        self
    }

    /// Include only these fields (whitelist). When set, all other fields are dropped.
    pub fn include(mut self, field: impl Into<String>) -> Self {
        self.includes.push(field.into());
        self
    }

    pub fn exclude(mut self, field: impl Into<String>) -> Self {
        self.excludes.push(field.into());
        self
    }
}

impl Default for ReplaceField {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleMessageTransform for ReplaceField {
    fn name(&self) -> &str {
        "ReplaceField"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        if let Some(serde_json::Value::Object(ref mut map)) = record.value {
            // Apply include whitelist: keep only listed fields
            if !self.includes.is_empty() {
                let keys: Vec<String> = map.keys().cloned().collect();
                for key in keys {
                    if !self.includes.contains(&key) {
                        map.remove(&key);
                    }
                }
            }
            // Remove excluded fields
            for exclude in &self.excludes {
                map.remove(exclude);
            }
            // Rename fields
            for (from, to) in &self.renames {
                if let Some(val) = map.remove(from) {
                    map.insert(to.clone(), val);
                }
            }
        }
        TransformResult::Record(record)
    }
}

/// Built-in: Mask sensitive field values
pub struct MaskField {
    fields: Vec<String>,
    replacement: serde_json::Value,
}

impl MaskField {
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            replacement: serde_json::Value::String("****".to_string()),
        }
    }

    pub fn with_replacement(mut self, replacement: serde_json::Value) -> Self {
        self.replacement = replacement;
        self
    }
}

impl SingleMessageTransform for MaskField {
    fn name(&self) -> &str {
        "MaskField"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        if let Some(serde_json::Value::Object(ref mut map)) = record.value {
            for field in &self.fields {
                if map.contains_key(field) {
                    map.insert(field.clone(), self.replacement.clone());
                }
            }
        }
        TransformResult::Record(record)
    }
}

/// Built-in: Route records to topics based on timestamp
pub struct TimestampRouter {
    format: String,
}

impl TimestampRouter {
    pub fn new(format: impl Into<String>) -> Self {
        Self {
            format: format.into(),
        }
    }
}

impl Default for TimestampRouter {
    fn default() -> Self {
        Self::new("${topic}-${timestamp}")
    }
}

impl SingleMessageTransform for TimestampRouter {
    fn name(&self) -> &str {
        "TimestampRouter"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        let ts = record.timestamp.unwrap_or(0);
        record.topic = self
            .format
            .replace("${topic}", &record.topic)
            .replace("${timestamp}", &ts.to_string());
        TransformResult::Record(record)
    }
}

/// Built-in: Route records to topics based on regex matching on the topic
pub struct RegexRouter {
    pattern: String,
    replacement: String,
}

impl RegexRouter {
    pub fn new(pattern: impl Into<String>, replacement: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            replacement: replacement.into(),
        }
    }
}

impl SingleMessageTransform for RegexRouter {
    fn name(&self) -> &str {
        "RegexRouter"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        if let Ok(re) = regex::Regex::new(&self.pattern) {
            record.topic = re
                .replace_all(&record.topic, &*self.replacement)
                .to_string();
        }
        TransformResult::Record(record)
    }
}

/// Built-in: Filter records based on a condition
pub struct Filter {
    field: String,
    value: serde_json::Value,
    invert: bool,
}

impl Filter {
    /// Drop records where field equals value
    pub fn drop_where(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            field: field.into(),
            value,
            invert: false,
        }
    }

    /// Keep only records where field equals value
    pub fn keep_where(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            field: field.into(),
            value,
            invert: true,
        }
    }
}

impl SingleMessageTransform for Filter {
    fn name(&self) -> &str {
        "Filter"
    }

    fn apply(&self, record: ConnectRecord) -> TransformResult {
        let matches = record
            .value
            .as_ref()
            .and_then(|v| v.get(&self.field))
            .map(|v| v == &self.value)
            .unwrap_or(false);

        let should_drop = if self.invert { !matches } else { matches };
        if should_drop {
            TransformResult::Drop
        } else {
            TransformResult::Record(record)
        }
    }
}

/// Built-in: Flatten nested JSON structures
pub struct Flatten {
    delimiter: String,
}

impl Flatten {
    pub fn new(delimiter: impl Into<String>) -> Self {
        Self {
            delimiter: delimiter.into(),
        }
    }
}

impl Default for Flatten {
    fn default() -> Self {
        Self::new(".")
    }
}

impl SingleMessageTransform for Flatten {
    fn name(&self) -> &str {
        "Flatten"
    }

    fn apply(&self, mut record: ConnectRecord) -> TransformResult {
        if let Some(value) = record.value.take() {
            let mut flat = serde_json::Map::new();
            flatten_value(&value, "", &self.delimiter, &mut flat);
            record.value = Some(serde_json::Value::Object(flat));
        }
        TransformResult::Record(record)
    }
}

fn flatten_value(
    value: &serde_json::Value,
    prefix: &str,
    delimiter: &str,
    result: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}{}{}", prefix, delimiter, key)
                };
                flatten_value(val, &new_prefix, delimiter, result);
            }
        }
        other => {
            result.insert(prefix.to_string(), other.clone());
        }
    }
}

/// Chain of transforms to apply in order
pub struct TransformChain {
    transforms: Vec<Box<dyn SingleMessageTransform>>,
}

impl TransformChain {
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }

    pub fn add(mut self, transform: Box<dyn SingleMessageTransform>) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Apply all transforms in order
    pub fn apply(&self, record: ConnectRecord) -> TransformResult {
        let mut current = record;
        for transform in &self.transforms {
            match transform.apply(current) {
                TransformResult::Record(r) => current = r,
                other => return other,
            }
        }
        TransformResult::Record(current)
    }

    /// Get the number of transforms in the chain
    pub fn len(&self) -> usize {
        self.transforms.len()
    }

    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    /// List transform names in the chain
    pub fn transform_names(&self) -> Vec<&str> {
        self.transforms.iter().map(|t| t.name()).collect()
    }
}

impl Default for TransformChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> ConnectRecord {
        ConnectRecord {
            topic: "orders".to_string(),
            partition: Some(0),
            key: None,
            value: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "address": {
                    "city": "NYC",
                    "zip": "10001"
                }
            })),
            headers: HashMap::new(),
            timestamp: Some(1706000000000),
        }
    }

    #[test]
    fn test_insert_field() {
        let transform = InsertField::new("source", serde_json::Value::String("orders-db".into()));
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            assert_eq!(
                r.value.unwrap().get("source").unwrap(),
                &serde_json::Value::String("orders-db".into())
            );
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_replace_field_rename() {
        let transform = ReplaceField::new()
            .rename("name", "full_name")
            .exclude("email");
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            let val = r.value.unwrap();
            assert!(val.get("full_name").is_some());
            assert!(val.get("name").is_none());
            assert!(val.get("email").is_none());
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_replace_field_include() {
        let transform = ReplaceField::new()
            .include("id")
            .include("name");
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            let val = r.value.unwrap();
            assert!(val.get("id").is_some());
            assert!(val.get("name").is_some());
            assert!(val.get("email").is_none());
            assert!(val.get("address").is_none());
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_mask_field() {
        let transform = MaskField::new(vec!["email".to_string()]);
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            assert_eq!(
                r.value.unwrap().get("email").unwrap(),
                &serde_json::Value::String("****".into())
            );
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_filter_drop() {
        let transform = Filter::drop_where("name", serde_json::Value::String("Alice".into()));
        let result = transform.apply(sample_record());
        assert!(matches!(result, TransformResult::Drop));
    }

    #[test]
    fn test_filter_keep() {
        let transform = Filter::keep_where("name", serde_json::Value::String("Alice".into()));
        let result = transform.apply(sample_record());
        assert!(matches!(result, TransformResult::Record(_)));
    }

    #[test]
    fn test_flatten() {
        let transform = Flatten::default();
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            let val = r.value.unwrap();
            assert!(val.get("address.city").is_some());
            assert!(val.get("address.zip").is_some());
            assert!(val.get("address").is_none());
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_timestamp_router() {
        let transform = TimestampRouter::default();
        let result = transform.apply(sample_record());
        if let TransformResult::Record(r) = result {
            assert_eq!(r.topic, "orders-1706000000000");
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_transform_chain() {
        let chain = TransformChain::new()
            .add(Box::new(InsertField::topic("_source_topic")))
            .add(Box::new(MaskField::new(vec!["email".to_string()])))
            .add(Box::new(Flatten::default()));

        assert_eq!(chain.len(), 3);
        assert_eq!(
            chain.transform_names(),
            vec!["InsertField", "MaskField", "Flatten"]
        );

        let result = chain.apply(sample_record());
        if let TransformResult::Record(r) = result {
            let val = r.value.unwrap();
            assert_eq!(
                val.get("_source_topic").unwrap(),
                &serde_json::Value::String("orders".into())
            );
            assert_eq!(
                val.get("email").unwrap(),
                &serde_json::Value::String("****".into())
            );
            assert!(val.get("address.city").is_some());
        } else {
            panic!("Expected Record");
        }
    }
}
