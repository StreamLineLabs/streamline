//! Entity definitions for the Feature Store
//!
//! Entities are the primary keys used to identify feature vectors.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Entity definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Entity name
    pub name: String,
    /// Entity key columns
    pub join_keys: Vec<String>,
    /// Value type
    pub value_type: EntityValueType,
    /// Description
    pub description: Option<String>,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Owner
    pub owner: Option<String>,
    /// Created timestamp
    pub created_at: i64,
    /// Updated timestamp
    pub updated_at: i64,
}

impl Entity {
    /// Create a new entity
    pub fn new(name: impl Into<String>, join_keys: Vec<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            name: name.into(),
            join_keys,
            value_type: EntityValueType::String,
            description: None,
            tags: HashMap::new(),
            owner: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set value type
    pub fn with_value_type(mut self, value_type: EntityValueType) -> Self {
        self.value_type = value_type;
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Set owner
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }
}

/// Entity value type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityValueType {
    /// String value
    #[default]
    String,
    /// Integer value
    Int32,
    /// Long value
    Int64,
    /// Float value
    Float,
    /// Double value
    Double,
    /// Bytes value
    Bytes,
}

/// Entity key for lookups
#[derive(Clone, Serialize, Deserialize)]
pub struct EntityKey {
    /// Entity type name
    pub entity_type: String,
    /// Key values
    pub keys: HashMap<String, EntityValue>,
}

impl EntityKey {
    /// Create a new entity key
    pub fn new(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            keys: HashMap::new(),
        }
    }

    /// Add a key value
    pub fn with_key(mut self, name: impl Into<String>, value: EntityValue) -> Self {
        self.keys.insert(name.into(), value);
        self
    }

    /// Get the composite key as a string
    pub fn to_key_string(&self) -> String {
        let mut parts: Vec<String> = self
            .keys
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        parts.sort();
        format!("{}:{}", self.entity_type, parts.join(","))
    }
}

impl fmt::Debug for EntityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityKey({})", self.to_key_string())
    }
}

impl Hash for EntityKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_key_string().hash(state);
    }
}

impl PartialEq for EntityKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_key_string() == other.to_key_string()
    }
}

impl Eq for EntityKey {}

/// Entity value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityValue {
    /// String value
    String(String),
    /// Integer value
    Int32(i32),
    /// Long value
    Int64(i64),
    /// Float value
    Float(f32),
    /// Double value
    Double(f64),
    /// Bytes value
    Bytes(Vec<u8>),
}

impl EntityValue {
    /// Check if the value matches the expected type
    pub fn matches_type(&self, value_type: EntityValueType) -> bool {
        matches!(
            (self, value_type),
            (EntityValue::String(_), EntityValueType::String)
                | (EntityValue::Int32(_), EntityValueType::Int32)
                | (EntityValue::Int64(_), EntityValueType::Int64)
                | (EntityValue::Float(_), EntityValueType::Float)
                | (EntityValue::Double(_), EntityValueType::Double)
                | (EntityValue::Bytes(_), EntityValueType::Bytes)
        )
    }

    /// Convert to string representation
    pub fn as_str(&self) -> Option<&str> {
        match self {
            EntityValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EntityValue::Int32(i) => Some(*i as i64),
            EntityValue::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Convert to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            EntityValue::Float(f) => Some(*f as f64),
            EntityValue::Double(d) => Some(*d),
            _ => None,
        }
    }
}

impl fmt::Display for EntityValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityValue::String(s) => write!(f, "{}", s),
            EntityValue::Int32(i) => write!(f, "{}", i),
            EntityValue::Int64(i) => write!(f, "{}", i),
            EntityValue::Float(v) => write!(f, "{}", v),
            EntityValue::Double(v) => write!(f, "{}", v),
            EntityValue::Bytes(b) => write!(f, "[{} bytes]", b.len()),
        }
    }
}

impl From<String> for EntityValue {
    fn from(s: String) -> Self {
        EntityValue::String(s)
    }
}

impl From<&str> for EntityValue {
    fn from(s: &str) -> Self {
        EntityValue::String(s.to_string())
    }
}

impl From<i32> for EntityValue {
    fn from(i: i32) -> Self {
        EntityValue::Int32(i)
    }
}

impl From<i64> for EntityValue {
    fn from(i: i64) -> Self {
        EntityValue::Int64(i)
    }
}

impl From<f32> for EntityValue {
    fn from(f: f32) -> Self {
        EntityValue::Float(f)
    }
}

impl From<f64> for EntityValue {
    fn from(f: f64) -> Self {
        EntityValue::Double(f)
    }
}

impl From<Vec<u8>> for EntityValue {
    fn from(b: Vec<u8>) -> Self {
        EntityValue::Bytes(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_creation() {
        let entity = Entity::new("user", vec!["user_id".to_string()])
            .with_value_type(EntityValueType::String)
            .with_description("User entity")
            .with_tag("team", "ml");

        assert_eq!(entity.name, "user");
        assert_eq!(entity.join_keys, vec!["user_id"]);
        assert_eq!(entity.value_type, EntityValueType::String);
        assert_eq!(entity.description, Some("User entity".to_string()));
        assert_eq!(entity.tags.get("team"), Some(&"ml".to_string()));
    }

    #[test]
    fn test_entity_key() {
        let key = EntityKey::new("user").with_key("user_id", EntityValue::String("123".into()));

        assert_eq!(key.entity_type, "user");
        assert_eq!(key.to_key_string(), "user:user_id=123");
    }

    #[test]
    fn test_entity_value_type_match() {
        assert!(EntityValue::String("test".into()).matches_type(EntityValueType::String));
        assert!(EntityValue::Int64(42).matches_type(EntityValueType::Int64));
        assert!(!EntityValue::String("test".into()).matches_type(EntityValueType::Int64));
    }
}
