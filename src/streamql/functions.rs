//! Built-in functions for StreamlineQL
//!
//! Provides scalar and aggregate functions for query processing.

use super::types::{DataType, Value};
use crate::error::{Result, StreamlineError};
use std::collections::HashMap;

/// Scalar function that operates on single values
#[derive(Debug, Clone)]
pub struct ScalarFunction {
    /// Function name
    pub name: String,
    /// Parameter types
    pub param_types: Vec<DataType>,
    /// Return type
    pub return_type: DataType,
    /// Implementation
    pub func: fn(&[Value]) -> Result<Value>,
}

/// Aggregate function that combines multiple values
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    /// Function name
    pub name: String,
    /// Parameter type
    pub param_type: DataType,
    /// Return type
    pub return_type: DataType,
}

/// Aggregate state for computing aggregate functions
#[derive(Debug, Clone)]
pub enum AggregateState {
    /// COUNT(*) or COUNT(column)
    Count {
        count: i64,
        distinct_values: Option<Vec<Value>>,
    },
    /// SUM aggregate
    Sum { sum: f64, is_null: bool },
    /// AVG aggregate
    Avg { sum: f64, count: i64 },
    /// MIN aggregate
    Min { min: Option<Value> },
    /// MAX aggregate
    Max { max: Option<Value> },
    /// FIRST aggregate
    First { first: Option<Value> },
    /// LAST aggregate
    Last { last: Option<Value> },
    /// STRING_AGG / LISTAGG
    StringAgg {
        values: Vec<String>,
        separator: String,
    },
    /// ARRAY_AGG
    ArrayAgg { values: Vec<Value> },
}

impl AggregateState {
    /// Create initial state for an aggregate function
    pub fn new(func_name: &str, distinct: bool) -> Result<Self> {
        match func_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateState::Count {
                count: 0,
                distinct_values: if distinct { Some(Vec::new()) } else { None },
            }),
            "SUM" => Ok(AggregateState::Sum {
                sum: 0.0,
                is_null: true,
            }),
            "AVG" => Ok(AggregateState::Avg { sum: 0.0, count: 0 }),
            "MIN" => Ok(AggregateState::Min { min: None }),
            "MAX" => Ok(AggregateState::Max { max: None }),
            "FIRST" | "FIRST_VALUE" => Ok(AggregateState::First { first: None }),
            "LAST" | "LAST_VALUE" => Ok(AggregateState::Last { last: None }),
            "STRING_AGG" | "LISTAGG" => Ok(AggregateState::StringAgg {
                values: Vec::new(),
                separator: ",".to_string(),
            }),
            "ARRAY_AGG" | "COLLECT_LIST" => Ok(AggregateState::ArrayAgg { values: Vec::new() }),
            _ => Err(StreamlineError::Parse(format!(
                "Unknown aggregate function: {}",
                func_name
            ))),
        }
    }

    /// Update the aggregate state with a new value
    pub fn update(&mut self, value: &Value) -> Result<()> {
        match self {
            AggregateState::Count {
                count,
                distinct_values,
            } => {
                if !value.is_null() {
                    if let Some(dv) = distinct_values {
                        if !dv.iter().any(|v| v == value) {
                            dv.push(value.clone());
                            *count += 1;
                        }
                    } else {
                        *count += 1;
                    }
                }
            }
            AggregateState::Sum { sum, is_null } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *is_null = false;
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *count += 1;
                }
            }
            AggregateState::Min { min } => {
                if !value.is_null() {
                    *min = Some(match min {
                        Some(current) => {
                            if value < current {
                                value.clone()
                            } else {
                                current.clone()
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            AggregateState::Max { max } => {
                if !value.is_null() {
                    *max = Some(match max {
                        Some(current) => {
                            if value > current {
                                value.clone()
                            } else {
                                current.clone()
                            }
                        }
                        None => value.clone(),
                    });
                }
            }
            AggregateState::First { first } => {
                if first.is_none() && !value.is_null() {
                    *first = Some(value.clone());
                }
            }
            AggregateState::Last { last } => {
                if !value.is_null() {
                    *last = Some(value.clone());
                }
            }
            AggregateState::StringAgg { values, .. } => {
                if let Some(s) = value.as_string() {
                    values.push(s);
                }
            }
            AggregateState::ArrayAgg { values } => {
                values.push(value.clone());
            }
        }
        Ok(())
    }

    /// Finalize the aggregate and return the result
    pub fn finalize(&self) -> Value {
        match self {
            AggregateState::Count { count, .. } => Value::Int64(*count),
            AggregateState::Sum { sum, is_null } => {
                if *is_null {
                    Value::Null
                } else {
                    Value::Float64(*sum)
                }
            }
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float64(*sum / *count as f64)
                }
            }
            AggregateState::Min { min } => min.clone().unwrap_or(Value::Null),
            AggregateState::Max { max } => max.clone().unwrap_or(Value::Null),
            AggregateState::First { first } => first.clone().unwrap_or(Value::Null),
            AggregateState::Last { last } => last.clone().unwrap_or(Value::Null),
            AggregateState::StringAgg { values, separator } => {
                Value::String(values.join(separator))
            }
            AggregateState::ArrayAgg { values } => Value::Array(values.clone()),
        }
    }

    /// Merge another aggregate state into this one
    pub fn merge(&mut self, other: &AggregateState) -> Result<()> {
        match (self, other) {
            (AggregateState::Count { count: c1, .. }, AggregateState::Count { count: c2, .. }) => {
                *c1 += c2;
            }
            (
                AggregateState::Sum {
                    sum: s1,
                    is_null: n1,
                },
                AggregateState::Sum {
                    sum: s2,
                    is_null: n2,
                },
            ) => {
                if !*n2 {
                    *s1 += s2;
                    *n1 = false;
                }
            }
            (
                AggregateState::Avg { sum: s1, count: c1 },
                AggregateState::Avg { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            }
            (AggregateState::Min { min: m1 }, AggregateState::Min { min: m2 }) => {
                if let Some(v2) = m2 {
                    *m1 = Some(match m1 {
                        Some(ref v1) if *v1 < *v2 => v1.clone(),
                        _ => v2.clone(),
                    });
                }
            }
            (AggregateState::Max { max: m1 }, AggregateState::Max { max: m2 }) => {
                if let Some(v2) = m2 {
                    *m1 = Some(match m1 {
                        Some(ref v1) if *v1 > *v2 => v1.clone(),
                        _ => v2.clone(),
                    });
                }
            }
            _ => {
                return Err(StreamlineError::Query(
                    "Cannot merge incompatible aggregate states".to_string(),
                ))
            }
        }
        Ok(())
    }
}

/// Registry of built-in functions
pub struct FunctionRegistry {
    /// Scalar functions by name
    scalar_functions: HashMap<String, ScalarFunction>,
    /// Aggregate functions by name
    aggregate_functions: HashMap<String, AggregateFunction>,
}

impl FunctionRegistry {
    /// Create a new function registry with built-in functions
    pub fn new() -> Self {
        let mut registry = Self {
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
        };
        registry.register_builtin_functions();
        registry
    }

    fn register_builtin_functions(&mut self) {
        // Register aggregate functions
        self.register_aggregate("COUNT", DataType::Null, DataType::Int64);
        self.register_aggregate("SUM", DataType::Float64, DataType::Float64);
        self.register_aggregate("AVG", DataType::Float64, DataType::Float64);
        self.register_aggregate("MIN", DataType::Null, DataType::Null);
        self.register_aggregate("MAX", DataType::Null, DataType::Null);
        self.register_aggregate("FIRST", DataType::Null, DataType::Null);
        self.register_aggregate("LAST", DataType::Null, DataType::Null);
        self.register_aggregate("FIRST_VALUE", DataType::Null, DataType::Null);
        self.register_aggregate("LAST_VALUE", DataType::Null, DataType::Null);
        self.register_aggregate("STRING_AGG", DataType::String, DataType::String);
        self.register_aggregate(
            "ARRAY_AGG",
            DataType::Null,
            DataType::Array(Box::new(DataType::Null)),
        );

        // Register scalar functions
        self.register_scalar(
            "UPPER",
            vec![DataType::String],
            DataType::String,
            scalar_upper,
        );
        self.register_scalar(
            "LOWER",
            vec![DataType::String],
            DataType::String,
            scalar_lower,
        );
        self.register_scalar(
            "LENGTH",
            vec![DataType::String],
            DataType::Int64,
            scalar_length,
        );
        self.register_scalar(
            "TRIM",
            vec![DataType::String],
            DataType::String,
            scalar_trim,
        );
        self.register_scalar(
            "LTRIM",
            vec![DataType::String],
            DataType::String,
            scalar_ltrim,
        );
        self.register_scalar(
            "RTRIM",
            vec![DataType::String],
            DataType::String,
            scalar_rtrim,
        );
        self.register_scalar(
            "CONCAT",
            vec![DataType::String, DataType::String],
            DataType::String,
            scalar_concat,
        );
        self.register_scalar(
            "SUBSTRING",
            vec![DataType::String, DataType::Int64, DataType::Int64],
            DataType::String,
            scalar_substring,
        );
        self.register_scalar(
            "REPLACE",
            vec![DataType::String, DataType::String, DataType::String],
            DataType::String,
            scalar_replace,
        );
        self.register_scalar(
            "COALESCE",
            vec![DataType::Null],
            DataType::Null,
            scalar_coalesce,
        );
        self.register_scalar(
            "NULLIF",
            vec![DataType::Null, DataType::Null],
            DataType::Null,
            scalar_nullif,
        );
        self.register_scalar(
            "ABS",
            vec![DataType::Float64],
            DataType::Float64,
            scalar_abs,
        );
        self.register_scalar(
            "FLOOR",
            vec![DataType::Float64],
            DataType::Int64,
            scalar_floor,
        );
        self.register_scalar(
            "CEIL",
            vec![DataType::Float64],
            DataType::Int64,
            scalar_ceil,
        );
        self.register_scalar(
            "ROUND",
            vec![DataType::Float64],
            DataType::Float64,
            scalar_round,
        );
        self.register_scalar("NOW", vec![], DataType::Timestamp, scalar_now);
        self.register_scalar("CURRENT_TIMESTAMP", vec![], DataType::Timestamp, scalar_now);
        self.register_scalar(
            "EXTRACT",
            vec![DataType::String, DataType::Timestamp],
            DataType::Int64,
            scalar_extract,
        );
    }

    fn register_scalar(
        &mut self,
        name: &str,
        param_types: Vec<DataType>,
        return_type: DataType,
        func: fn(&[Value]) -> Result<Value>,
    ) {
        self.scalar_functions.insert(
            name.to_uppercase(),
            ScalarFunction {
                name: name.to_string(),
                param_types,
                return_type,
                func,
            },
        );
    }

    fn register_aggregate(&mut self, name: &str, param_type: DataType, return_type: DataType) {
        self.aggregate_functions.insert(
            name.to_uppercase(),
            AggregateFunction {
                name: name.to_string(),
                param_type,
                return_type,
            },
        );
    }

    /// Get a scalar function by name
    pub fn get_scalar(&self, name: &str) -> Option<&ScalarFunction> {
        self.scalar_functions.get(&name.to_uppercase())
    }

    /// Get an aggregate function by name
    pub fn get_aggregate(&self, name: &str) -> Option<&AggregateFunction> {
        self.aggregate_functions.get(&name.to_uppercase())
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.aggregate_functions.contains_key(&name.to_uppercase())
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Scalar function implementations

fn scalar_upper(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "UPPER requires string argument".to_string(),
        )),
    }
}

fn scalar_lower(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "LOWER requires string argument".to_string(),
        )),
    }
}

fn scalar_length(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::Int64(s.len() as i64)),
        Some(Value::Binary(b)) => Ok(Value::Int64(b.len() as i64)),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "LENGTH requires string or binary argument".to_string(),
        )),
    }
}

fn scalar_trim(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "TRIM requires string argument".to_string(),
        )),
    }
}

fn scalar_ltrim(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim_start().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "LTRIM requires string argument".to_string(),
        )),
    }
}

fn scalar_rtrim(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim_end().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "RTRIM requires string argument".to_string(),
        )),
    }
}

fn scalar_concat(args: &[Value]) -> Result<Value> {
    let mut result = String::new();
    for arg in args {
        match arg {
            Value::String(s) => result.push_str(s),
            Value::Null => {}
            other => result.push_str(&other.to_string()),
        }
    }
    Ok(Value::String(result))
}

fn scalar_substring(args: &[Value]) -> Result<Value> {
    match (args.first(), args.get(1), args.get(2)) {
        (Some(Value::String(s)), Some(Value::Int64(start)), Some(Value::Int64(len))) => {
            let start = (*start - 1).max(0) as usize;
            let len = (*len).max(0) as usize;
            let result: String = s.chars().skip(start).take(len).collect();
            Ok(Value::String(result))
        }
        (Some(Value::Null), _, _) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "SUBSTRING requires (string, start, length) arguments".to_string(),
        )),
    }
}

fn scalar_replace(args: &[Value]) -> Result<Value> {
    match (args.first(), args.get(1), args.get(2)) {
        (Some(Value::String(s)), Some(Value::String(from)), Some(Value::String(to))) => {
            Ok(Value::String(s.replace(from, to)))
        }
        (Some(Value::Null), _, _) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "REPLACE requires (string, from, to) arguments".to_string(),
        )),
    }
}

fn scalar_coalesce(args: &[Value]) -> Result<Value> {
    for arg in args {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

fn scalar_nullif(args: &[Value]) -> Result<Value> {
    match (args.first(), args.get(1)) {
        (Some(a), Some(b)) if a == b => Ok(Value::Null),
        (Some(a), _) => Ok(a.clone()),
        _ => Err(StreamlineError::Query(
            "NULLIF requires 2 arguments".to_string(),
        )),
    }
}

fn scalar_abs(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
        Some(Value::Float64(n)) => Ok(Value::Float64(n.abs())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "ABS requires numeric argument".to_string(),
        )),
    }
}

fn scalar_floor(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Float64(n)) => Ok(Value::Int64(n.floor() as i64)),
        Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "FLOOR requires numeric argument".to_string(),
        )),
    }
}

fn scalar_ceil(args: &[Value]) -> Result<Value> {
    match args.first() {
        Some(Value::Float64(n)) => Ok(Value::Int64(n.ceil() as i64)),
        Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "CEIL requires numeric argument".to_string(),
        )),
    }
}

fn scalar_round(args: &[Value]) -> Result<Value> {
    match (args.first(), args.get(1)) {
        (Some(Value::Float64(n)), Some(Value::Int64(precision))) => {
            let factor = 10_f64.powi(*precision as i32);
            Ok(Value::Float64((n * factor).round() / factor))
        }
        (Some(Value::Float64(n)), None) => Ok(Value::Float64(n.round())),
        (Some(Value::Int64(n)), _) => Ok(Value::Int64(*n)),
        (Some(Value::Null), _) => Ok(Value::Null),
        _ => Err(StreamlineError::Query(
            "ROUND requires numeric argument".to_string(),
        )),
    }
}

fn scalar_now(_args: &[Value]) -> Result<Value> {
    Ok(Value::Timestamp(chrono::Utc::now().timestamp_millis()))
}

fn scalar_extract(args: &[Value]) -> Result<Value> {
    use chrono::{Datelike, TimeZone, Timelike, Utc};

    match (args.first(), args.get(1)) {
        (Some(Value::String(part)), Some(Value::Timestamp(ts))) => {
            let dt = Utc
                .timestamp_millis_opt(*ts)
                .single()
                .ok_or_else(|| StreamlineError::Query("Invalid timestamp".to_string()))?;

            let value = match part.to_uppercase().as_str() {
                "YEAR" => dt.year() as i64,
                "MONTH" => dt.month() as i64,
                "DAY" => dt.day() as i64,
                "HOUR" => dt.hour() as i64,
                "MINUTE" => dt.minute() as i64,
                "SECOND" => dt.second() as i64,
                "DOW" | "DAYOFWEEK" => dt.weekday().num_days_from_sunday() as i64,
                "DOY" | "DAYOFYEAR" => dt.ordinal() as i64,
                "WEEK" => dt.iso_week().week() as i64,
                "QUARTER" => ((dt.month() - 1) / 3 + 1) as i64,
                "EPOCH" => dt.timestamp(),
                _ => {
                    return Err(StreamlineError::Query(format!(
                        "Unknown extract part: {}",
                        part
                    )))
                }
            };
            Ok(Value::Int64(value))
        }
        _ => Err(StreamlineError::Query(
            "EXTRACT requires (part, timestamp) arguments".to_string(),
        )),
    }
}

// ============================================================================
// User-Defined Functions (UDF) Framework
// ============================================================================

/// User-defined scalar function
///
/// UDFs allow users to register custom functions that can be used in StreamQL queries.
/// Each UDF has a name, parameter types, return type, and an implementation closure.
pub struct UserDefinedFunction {
    /// Function name (case-insensitive, will be uppercased)
    pub name: String,
    /// Parameter types
    pub param_types: Vec<DataType>,
    /// Return type
    pub return_type: DataType,
    /// Description for documentation
    pub description: String,
    /// Implementation
    func: UserDefinedFunctionImpl,
}

type UserDefinedFunctionImpl = Box<dyn Fn(&[Value]) -> Result<Value> + Send + Sync>;

impl std::fmt::Debug for UserDefinedFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserDefinedFunction")
            .field("name", &self.name)
            .field("param_types", &self.param_types)
            .field("return_type", &self.return_type)
            .field("description", &self.description)
            .finish()
    }
}

impl UserDefinedFunction {
    /// Create a new user-defined function
    pub fn new<F>(
        name: impl Into<String>,
        param_types: Vec<DataType>,
        return_type: DataType,
        description: impl Into<String>,
        func: F,
    ) -> Self
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            param_types,
            return_type,
            description: description.into(),
            func: Box::new(func),
        }
    }

    /// Execute the UDF with the given arguments
    pub fn call(&self, args: &[Value]) -> Result<Value> {
        (self.func)(args)
    }
}

/// Registry for user-defined functions
///
/// This registry allows users to register and manage custom functions
/// that can be used in StreamQL queries.
pub struct UdfRegistry {
    /// User-defined scalar functions by name
    udfs: HashMap<String, UserDefinedFunction>,
}

impl UdfRegistry {
    /// Create a new empty UDF registry
    pub fn new() -> Self {
        Self {
            udfs: HashMap::new(),
        }
    }

    /// Register a user-defined function
    ///
    /// # Arguments
    /// * `udf` - The user-defined function to register
    ///
    /// # Returns
    /// * `Ok(())` if registered successfully
    /// * `Err` if a function with the same name already exists
    pub fn register(&mut self, udf: UserDefinedFunction) -> Result<()> {
        let name = udf.name.to_uppercase();
        if self.udfs.contains_key(&name) {
            return Err(StreamlineError::Query(format!(
                "UDF '{}' already registered",
                name
            )));
        }
        self.udfs.insert(name, udf);
        Ok(())
    }

    /// Unregister a user-defined function
    pub fn unregister(&mut self, name: &str) -> bool {
        self.udfs.remove(&name.to_uppercase()).is_some()
    }

    /// Get a user-defined function by name
    pub fn get(&self, name: &str) -> Option<&UserDefinedFunction> {
        self.udfs.get(&name.to_uppercase())
    }

    /// Check if a UDF exists
    pub fn contains(&self, name: &str) -> bool {
        self.udfs.contains_key(&name.to_uppercase())
    }

    /// List all registered UDFs
    pub fn list(&self) -> Vec<&UserDefinedFunction> {
        self.udfs.values().collect()
    }

    /// Get the number of registered UDFs
    pub fn len(&self) -> usize {
        self.udfs.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.udfs.is_empty()
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating user-defined functions with a fluent API
pub struct UdfBuilder {
    name: Option<String>,
    param_types: Vec<DataType>,
    return_type: Option<DataType>,
    description: String,
}

impl UdfBuilder {
    /// Start building a new UDF
    pub fn new() -> Self {
        Self {
            name: None,
            param_types: Vec::new(),
            return_type: None,
            description: String::new(),
        }
    }

    /// Set the function name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add a parameter type
    pub fn param(mut self, param_type: DataType) -> Self {
        self.param_types.push(param_type);
        self
    }

    /// Set multiple parameter types
    pub fn params(mut self, param_types: Vec<DataType>) -> Self {
        self.param_types = param_types;
        self
    }

    /// Set the return type
    pub fn returns(mut self, return_type: DataType) -> Self {
        self.return_type = Some(return_type);
        self
    }

    /// Set the description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Build the UDF with the given implementation
    pub fn build<F>(self, func: F) -> Result<UserDefinedFunction>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static,
    {
        let name = self
            .name
            .ok_or_else(|| StreamlineError::Query("UDF name is required".to_string()))?;
        let return_type = self
            .return_type
            .ok_or_else(|| StreamlineError::Query("UDF return type is required".to_string()))?;

        Ok(UserDefinedFunction::new(
            name,
            self.param_types,
            return_type,
            self.description,
            func,
        ))
    }
}

impl Default for UdfBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Extended function registry that includes UDFs
///
/// This combines the built-in function registry with user-defined functions,
/// providing a unified interface for function lookup.
pub struct ExtendedFunctionRegistry {
    /// Built-in functions
    builtin: FunctionRegistry,
    /// User-defined functions
    udf: UdfRegistry,
}

impl ExtendedFunctionRegistry {
    /// Create a new extended registry with built-in functions
    pub fn new() -> Self {
        Self {
            builtin: FunctionRegistry::new(),
            udf: UdfRegistry::new(),
        }
    }

    /// Register a user-defined function
    pub fn register_udf(&mut self, udf: UserDefinedFunction) -> Result<()> {
        // Check if name conflicts with built-in
        let name = udf.name.to_uppercase();
        if self.builtin.get_scalar(&name).is_some() || self.builtin.get_aggregate(&name).is_some() {
            return Err(StreamlineError::Query(format!(
                "Cannot register UDF '{}': conflicts with built-in function",
                name
            )));
        }
        self.udf.register(udf)
    }

    /// Get a scalar function (built-in or UDF)
    pub fn get_scalar(&self, name: &str) -> Option<ScalarFunctionRef<'_>> {
        if let Some(builtin) = self.builtin.get_scalar(name) {
            return Some(ScalarFunctionRef::Builtin(builtin));
        }
        if let Some(udf) = self.udf.get(name) {
            return Some(ScalarFunctionRef::Udf(udf));
        }
        None
    }

    /// Get an aggregate function
    pub fn get_aggregate(&self, name: &str) -> Option<&AggregateFunction> {
        self.builtin.get_aggregate(name)
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.builtin.is_aggregate(name)
    }

    /// List all UDFs
    pub fn list_udfs(&self) -> Vec<&UserDefinedFunction> {
        self.udf.list()
    }

    /// Unregister a UDF
    pub fn unregister_udf(&mut self, name: &str) -> bool {
        self.udf.unregister(name)
    }
}

impl Default for ExtendedFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Reference to either a built-in scalar function or a UDF
pub enum ScalarFunctionRef<'a> {
    Builtin(&'a ScalarFunction),
    Udf(&'a UserDefinedFunction),
}

impl<'a> ScalarFunctionRef<'a> {
    /// Execute the function with the given arguments
    pub fn call(&self, args: &[Value]) -> Result<Value> {
        match self {
            ScalarFunctionRef::Builtin(f) => (f.func)(args),
            ScalarFunctionRef::Udf(f) => f.call(args),
        }
    }

    /// Get the function name
    pub fn name(&self) -> &str {
        match self {
            ScalarFunctionRef::Builtin(f) => &f.name,
            ScalarFunctionRef::Udf(f) => &f.name,
        }
    }

    /// Get the return type
    pub fn return_type(&self) -> &DataType {
        match self {
            ScalarFunctionRef::Builtin(f) => &f.return_type,
            ScalarFunctionRef::Udf(f) => &f.return_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_count() {
        let mut state = AggregateState::new("COUNT", false).unwrap();
        state.update(&Value::Int64(1)).unwrap();
        state.update(&Value::Int64(2)).unwrap();
        state.update(&Value::Null).unwrap();
        state.update(&Value::Int64(3)).unwrap();

        assert_eq!(state.finalize(), Value::Int64(3));
    }

    #[test]
    fn test_aggregate_sum() {
        let mut state = AggregateState::new("SUM", false).unwrap();
        state.update(&Value::Int64(10)).unwrap();
        state.update(&Value::Int64(20)).unwrap();
        state.update(&Value::Float64(5.5)).unwrap();

        assert_eq!(state.finalize(), Value::Float64(35.5));
    }

    #[test]
    fn test_aggregate_avg() {
        let mut state = AggregateState::new("AVG", false).unwrap();
        state.update(&Value::Float64(10.0)).unwrap();
        state.update(&Value::Float64(20.0)).unwrap();
        state.update(&Value::Float64(30.0)).unwrap();

        assert_eq!(state.finalize(), Value::Float64(20.0));
    }

    #[test]
    fn test_scalar_upper() {
        let result = scalar_upper(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_scalar_length() {
        let result = scalar_length(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Int64(5));
    }

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::new();
        assert!(registry.is_aggregate("COUNT"));
        assert!(registry.is_aggregate("SUM"));
        assert!(!registry.is_aggregate("UPPER"));
        assert!(registry.get_scalar("UPPER").is_some());
    }

    #[test]
    fn test_udf_registration() {
        let mut registry = UdfRegistry::new();

        // Create a simple UDF that doubles a number
        let udf = UserDefinedFunction::new(
            "double",
            vec![DataType::Int64],
            DataType::Int64,
            "Doubles the input value",
            |args| match args.first() {
                Some(Value::Int64(n)) => Ok(Value::Int64(n * 2)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Err(StreamlineError::Query("Expected integer".to_string())),
            },
        );

        registry.register(udf).unwrap();
        assert!(registry.contains("DOUBLE"));
        assert!(registry.contains("double")); // Case-insensitive

        // Execute the UDF
        let func = registry.get("double").unwrap();
        let result = func.call(&[Value::Int64(21)]).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_udf_builder() {
        let udf = UdfBuilder::new()
            .name("triple")
            .param(DataType::Int64)
            .returns(DataType::Int64)
            .description("Triples the input value")
            .build(|args| match args.first() {
                Some(Value::Int64(n)) => Ok(Value::Int64(n * 3)),
                _ => Ok(Value::Null),
            })
            .unwrap();

        assert_eq!(udf.name, "triple");
        let result = udf.call(&[Value::Int64(10)]).unwrap();
        assert_eq!(result, Value::Int64(30));
    }

    #[test]
    fn test_extended_registry() {
        let mut registry = ExtendedFunctionRegistry::new();

        // Built-in function should be available
        assert!(registry.get_scalar("UPPER").is_some());
        assert!(registry.is_aggregate("COUNT"));

        // Register a UDF
        let udf = UserDefinedFunction::new(
            "custom_func",
            vec![DataType::String],
            DataType::String,
            "Custom function",
            |args| match args.first() {
                Some(Value::String(s)) => Ok(Value::String(format!("custom: {}", s))),
                _ => Ok(Value::Null),
            },
        );
        registry.register_udf(udf).unwrap();

        // UDF should be available
        let func = registry.get_scalar("custom_func").unwrap();
        let result = func.call(&[Value::String("test".to_string())]).unwrap();
        assert_eq!(result, Value::String("custom: test".to_string()));

        // Cannot register UDF with built-in name
        let conflict_udf = UserDefinedFunction::new(
            "UPPER",
            vec![DataType::String],
            DataType::String,
            "Conflicting",
            |_| Ok(Value::Null),
        );
        assert!(registry.register_udf(conflict_udf).is_err());
    }

    #[test]
    fn test_udf_unregister() {
        let mut registry = UdfRegistry::new();

        let udf =
            UserDefinedFunction::new("temp_func", vec![], DataType::Int64, "Temporary", |_| {
                Ok(Value::Int64(42))
            });

        registry.register(udf).unwrap();
        assert!(registry.contains("temp_func"));

        registry.unregister("temp_func");
        assert!(!registry.contains("temp_func"));
    }
}
