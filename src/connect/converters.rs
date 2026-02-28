//! Connect data converters for serialization/deserialization.
//!
//! Implements key converters compatible with Kafka Connect:
//! JsonConverter, StringConverter, ByteArrayConverter.

use serde_json::Value;

/// Trait for converting between wire format and internal representation.
pub trait Converter: Send + Sync {
    /// Converter name.
    fn name(&self) -> &str;
    /// Serialize a value to bytes.
    fn serialize(&self, topic: &str, value: &Value) -> Result<Vec<u8>, String>;
    /// Deserialize bytes to a value.
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<Value, String>;
}

/// JSON converter — serializes/deserializes as JSON.
pub struct JsonConverter {
    schemas_enabled: bool,
}

impl JsonConverter {
    pub fn new(schemas_enabled: bool) -> Self {
        Self { schemas_enabled }
    }
}

impl Converter for JsonConverter {
    fn name(&self) -> &str { "JsonConverter" }

    fn serialize(&self, _topic: &str, value: &Value) -> Result<Vec<u8>, String> {
        if self.schemas_enabled {
            let envelope = serde_json::json!({
                "schema": null,
                "payload": value
            });
            serde_json::to_vec(&envelope).map_err(|e| e.to_string())
        } else {
            serde_json::to_vec(value).map_err(|e| e.to_string())
        }
    }

    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Value, String> {
        let value: Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
        if self.schemas_enabled {
            if let Some(payload) = value.get("payload") {
                Ok(payload.clone())
            } else {
                Ok(value)
            }
        } else {
            Ok(value)
        }
    }
}

/// String converter — treats values as UTF-8 strings.
pub struct StringConverter;

impl Converter for StringConverter {
    fn name(&self) -> &str { "StringConverter" }

    fn serialize(&self, _topic: &str, value: &Value) -> Result<Vec<u8>, String> {
        match value {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            other => Ok(other.to_string().into_bytes()),
        }
    }

    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Value, String> {
        let s = std::str::from_utf8(data).map_err(|e| e.to_string())?;
        Ok(Value::String(s.to_string()))
    }
}

/// ByteArray converter — passes bytes through as base64.
pub struct ByteArrayConverter;

impl Converter for ByteArrayConverter {
    fn name(&self) -> &str { "ByteArrayConverter" }

    fn serialize(&self, _topic: &str, value: &Value) -> Result<Vec<u8>, String> {
        match value {
            Value::String(s) => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(|e| e.to_string())
            }
            _ => Err("ByteArrayConverter expects base64-encoded string".to_string()),
        }
    }

    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Value, String> {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
        Ok(Value::String(encoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_converter_roundtrip() {
        let converter = JsonConverter::new(false);
        let value = json!({"name": "Alice", "age": 30});
        let bytes = converter.serialize("test", &value).unwrap();
        let result = converter.deserialize("test", &bytes).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_json_converter_with_schema() {
        let converter = JsonConverter::new(true);
        let value = json!({"name": "Alice"});
        let bytes = converter.serialize("test", &value).unwrap();
        let result = converter.deserialize("test", &bytes).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_string_converter_roundtrip() {
        let converter = StringConverter;
        let value = json!("hello world");
        let bytes = converter.serialize("test", &value).unwrap();
        assert_eq!(bytes, b"hello world");
        let result = converter.deserialize("test", &bytes).unwrap();
        assert_eq!(result, json!("hello world"));
    }

    #[test]
    fn test_string_converter_non_string() {
        let converter = StringConverter;
        let value = json!(42);
        let bytes = converter.serialize("test", &value).unwrap();
        let result = converter.deserialize("test", &bytes).unwrap();
        assert_eq!(result, json!("42"));
    }

    #[test]
    fn test_byte_array_converter_roundtrip() {
        let converter = ByteArrayConverter;
        let original = b"binary data";
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(original);
        let value = json!(encoded);
        let bytes = converter.serialize("test", &value).unwrap();
        assert_eq!(bytes, original);
        let result = converter.deserialize("test", &bytes).unwrap();
        assert_eq!(result.as_str().unwrap(), encoded);
    }
}
