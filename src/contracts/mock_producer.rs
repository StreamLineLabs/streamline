//! Mock message producer for contract testing.

use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for mock data generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockProducerConfig {
    /// Number of messages to generate
    pub count: u64,
    /// Message template with variable substitution
    pub template: Option<String>,
    /// Key template
    pub key_template: Option<String>,
    /// Target partition
    pub partition: Option<i32>,
    /// Delay between messages in ms (for rate-limited testing)
    pub delay_ms: Option<u64>,
}

impl Default for MockProducerConfig {
    fn default() -> Self {
        Self {
            count: 100,
            template: None,
            key_template: None,
            partition: None,
            delay_ms: None,
        }
    }
}

/// A generated mock message.
#[derive(Debug, Clone)]
pub struct MockMessage {
    /// Message key
    pub key: Option<Bytes>,
    /// Message value
    pub value: Bytes,
    /// Target partition
    pub partition: i32,
    /// Sequence number
    pub sequence: u64,
}

/// Generates mock messages from contract specifications.
pub struct MockProducer {
    config: MockProducerConfig,
}

impl MockProducer {
    /// Create a new mock producer with the given configuration.
    pub fn new(config: MockProducerConfig) -> Self {
        Self { config }
    }

    /// Generate all mock messages.
    pub fn generate(&self) -> Result<Vec<MockMessage>> {
        let mut messages = Vec::with_capacity(self.config.count as usize);

        for i in 0..self.config.count {
            let value = match &self.config.template {
                Some(tpl) => self.expand_template(tpl, i),
                None => format!("{{\"sequence\":{},\"timestamp\":{}}}", i, Self::now_ms()),
            };

            let key = self
                .config
                .key_template
                .as_ref()
                .map(|tpl| Bytes::from(self.expand_template(tpl, i)));

            messages.push(MockMessage {
                key,
                value: Bytes::from(value),
                partition: self.config.partition.unwrap_or(0),
                sequence: i,
            });
        }

        Ok(messages)
    }

    fn expand_template(&self, template: &str, iteration: u64) -> String {
        let mut result = template.to_string();

        // {{uuid}}
        if result.contains("{{uuid}}") {
            result = result.replace("{{uuid}}", &uuid::Uuid::new_v4().to_string());
        }

        // {{timestamp}}
        if result.contains("{{timestamp}}") {
            result = result.replace("{{timestamp}}", &Self::now_ms().to_string());
        }

        // {{i}}
        if result.contains("{{i}}") {
            result = result.replace("{{i}}", &iteration.to_string());
        }

        // {{random:min:max}}
        while let Some(start) = result.find("{{random:") {
            if let Some(end) = result[start..].find("}}") {
                let spec = &result[start + 9..start + end];
                let parts: Vec<&str> = spec.split(':').collect();
                let replacement = if parts.len() == 2 {
                    let min: i64 = parts[0].parse().unwrap_or(0);
                    let max: i64 = parts[1].parse().unwrap_or(100);
                    let val = min + (rand::random::<u64>() % (max - min + 1) as u64) as i64;
                    val.to_string()
                } else {
                    "0".to_string()
                };
                result = format!(
                    "{}{}{}",
                    &result[..start],
                    replacement,
                    &result[start + end + 2..]
                );
            } else {
                break;
            }
        }

        result
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_default() {
        let producer = MockProducer::new(MockProducerConfig {
            count: 10,
            ..Default::default()
        });
        let messages = producer.generate().unwrap();
        assert_eq!(messages.len(), 10);
        for (i, msg) in messages.iter().enumerate() {
            assert_eq!(msg.sequence, i as u64);
        }
    }

    #[test]
    fn test_template_expansion() {
        let producer = MockProducer::new(MockProducerConfig {
            count: 3,
            template: Some(r#"{"id":"{{uuid}}","seq":{{i}}}"#.to_string()),
            ..Default::default()
        });
        let messages = producer.generate().unwrap();
        assert_eq!(messages.len(), 3);
        let val = String::from_utf8(messages[1].value.to_vec()).unwrap();
        assert!(val.contains("\"seq\":1"));
        assert!(val.contains("\"id\":\""));
    }
}
