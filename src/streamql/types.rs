//! Type system for StreamlineQL
//!
//! Defines the data types and values used throughout the query engine.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Data types supported by StreamlineQL
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Null type
    Null,
    /// Boolean (true/false)
    Boolean,
    /// 64-bit signed integer
    Int64,
    /// 64-bit floating point
    Float64,
    /// UTF-8 string
    String,
    /// Binary data
    Binary,
    /// Timestamp with millisecond precision
    Timestamp,
    /// Date (days since epoch)
    Date,
    /// Time duration in milliseconds
    Duration,
    /// JSON value
    Json,
    /// Array of values
    Array(Box<DataType>),
    /// Map of string keys to values
    Map(Box<DataType>),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::String => write!(f, "STRING"),
            DataType::Binary => write!(f, "BINARY"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Date => write!(f, "DATE"),
            DataType::Duration => write!(f, "DURATION"),
            DataType::Json => write!(f, "JSON"),
            DataType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            DataType::Map(inner) => write!(f, "MAP<STRING, {}>", inner),
        }
    }
}

/// Runtime value in StreamlineQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// Integer value
    Int64(i64),
    /// Floating point value
    Float64(f64),
    /// String value
    String(String),
    /// Binary value
    Binary(Vec<u8>),
    /// Timestamp (milliseconds since epoch)
    Timestamp(i64),
    /// Date (days since epoch)
    Date(i32),
    /// Duration (milliseconds)
    Duration(i64),
    /// JSON value (stored as string)
    Json(String),
    /// Array of values
    Array(Vec<Value>),
    /// Map of values
    Map(Vec<(String, Value)>),
}

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::String(_) => DataType::String,
            Value::Binary(_) => DataType::Binary,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Date(_) => DataType::Date,
            Value::Duration(_) => DataType::Duration,
            Value::Json(_) => DataType::Json,
            Value::Array(arr) => {
                let inner_type = arr.first().map(|v| v.data_type()).unwrap_or(DataType::Null);
                DataType::Array(Box::new(inner_type))
            }
            Value::Map(map) => {
                let inner_type = map
                    .first()
                    .map(|(_, v)| v.data_type())
                    .unwrap_or(DataType::Null);
                DataType::Map(Box::new(inner_type))
            }
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to convert to boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Int64(i) => Some(*i != 0),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(i) => Some(*i),
            Value::Float64(f) => Some(*f as i64),
            Value::String(s) => s.parse().ok(),
            Value::Timestamp(t) => Some(*t),
            Value::Duration(d) => Some(*d),
            _ => None,
        }
    }

    /// Try to convert to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(f) => Some(*f),
            Value::Int64(i) => Some(*i as f64),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to convert to string
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            Value::Json(j) => Some(j.clone()),
            Value::Int64(i) => Some(i.to_string()),
            Value::Float64(f) => Some(f.to_string()),
            Value::Boolean(b) => Some(b.to_string()),
            _ => None,
        }
    }

    /// Cast value to another type
    pub fn cast(&self, target: &DataType) -> Option<Value> {
        match (self, target) {
            // Identity casts
            (Value::Null, _) => Some(Value::Null),
            (v, t) if v.data_type() == *t => Some(v.clone()),

            // Boolean casts
            (Value::Boolean(b), DataType::Int64) => Some(Value::Int64(if *b { 1 } else { 0 })),
            (Value::Boolean(b), DataType::String) => Some(Value::String(b.to_string())),

            // Int64 casts
            (Value::Int64(i), DataType::Boolean) => Some(Value::Boolean(*i != 0)),
            (Value::Int64(i), DataType::Float64) => Some(Value::Float64(*i as f64)),
            (Value::Int64(i), DataType::String) => Some(Value::String(i.to_string())),
            (Value::Int64(i), DataType::Timestamp) => Some(Value::Timestamp(*i)),

            // Float64 casts
            (Value::Float64(f), DataType::Int64) => Some(Value::Int64(*f as i64)),
            (Value::Float64(f), DataType::String) => Some(Value::String(f.to_string())),

            // String casts
            (Value::String(s), DataType::Boolean) => s.parse().ok().map(Value::Boolean),
            (Value::String(s), DataType::Int64) => s.parse().ok().map(Value::Int64),
            (Value::String(s), DataType::Float64) => s.parse().ok().map(Value::Float64),
            (Value::String(s), DataType::Json) => Some(Value::Json(s.clone())),

            // Timestamp casts
            (Value::Timestamp(t), DataType::Int64) => Some(Value::Int64(*t)),
            (Value::Timestamp(t), DataType::String) => Some(Value::String(t.to_string())),

            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Binary(a), Value::Binary(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Duration(a), Value::Duration(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            // Cross-type comparisons for numeric types
            (Value::Int64(a), Value::Float64(b)) => (*a as f64) == *b,
            (Value::Float64(a), Value::Int64(b)) => *a == (*b as f64),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Int64(i) => i.hash(state),
            Value::Float64(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Binary(b) => b.hash(state),
            Value::Timestamp(t) => t.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Duration(d) => d.hash(state),
            Value::Json(j) => j.hash(state),
            Value::Array(arr) => arr.hash(state),
            Value::Map(map) => {
                for (k, v) in map {
                    k.hash(state);
                    v.hash(state);
                }
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Duration(a), Value::Duration(b)) => a.partial_cmp(b),
            // Cross-type comparisons for numeric types
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float64(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "'{}'", s),
            Value::Binary(b) => write!(f, "0x{}", hex::encode(b)),
            Value::Timestamp(t) => write!(f, "TIMESTAMP({})", t),
            Value::Date(d) => write!(f, "DATE({})", d),
            Value::Duration(d) => write!(f, "DURATION({}ms)", d),
            Value::Json(j) => write!(f, "{}", j),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Map(map) => {
                write!(f, "{{")?;
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}': {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

/// A row of values with column metadata
#[derive(Debug, Clone)]
pub struct Row {
    /// Column values
    pub values: Vec<Value>,
    /// Event timestamp (if available)
    pub event_time: Option<i64>,
}

impl Row {
    /// Create a new row
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values,
            event_time: None,
        }
    }

    /// Create a row with event time
    pub fn with_event_time(values: Vec<Value>, event_time: i64) -> Self {
        Self {
            values,
            event_time: Some(event_time),
        }
    }

    /// Get a value by column index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// Schema for a stream or query result
#[derive(Debug, Clone)]
pub struct Schema {
    /// Column definitions
    pub columns: Vec<Column>,
}

impl Schema {
    /// Create a new schema
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Get the number of columns
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if the schema is empty
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Find a column by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get a column by index
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }
}

/// Column definition
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: DataType,
    /// Whether the column can be null
    pub nullable: bool,
}

impl Column {
    /// Create a new column
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    /// Create a non-nullable column
    pub fn non_null(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_types() {
        assert_eq!(Value::Int64(42).data_type(), DataType::Int64);
        assert_eq!(
            Value::String("hello".to_string()).data_type(),
            DataType::String
        );
        assert_eq!(Value::Boolean(true).data_type(), DataType::Boolean);
    }

    #[test]
    fn test_value_casts() {
        let int_val = Value::Int64(42);
        assert_eq!(int_val.cast(&DataType::Float64), Some(Value::Float64(42.0)));
        assert_eq!(
            int_val.cast(&DataType::String),
            Some(Value::String("42".to_string()))
        );

        let str_val = Value::String("123".to_string());
        assert_eq!(str_val.cast(&DataType::Int64), Some(Value::Int64(123)));
    }

    #[test]
    fn test_value_comparisons() {
        assert!(Value::Int64(10) < Value::Int64(20));
        assert!(Value::String("apple".to_string()) < Value::String("banana".to_string()));
        assert_eq!(Value::Int64(42), Value::Float64(42.0));
    }

    #[test]
    fn test_schema() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int64),
            Column::new("name", DataType::String),
        ]);

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("missing"), None);
    }
}
