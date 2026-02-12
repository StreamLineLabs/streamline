//! User-Defined Functions (UDFs) for StreamQL
//!
//! Provides a registry for custom scalar and aggregate functions that
//! extend the built-in StreamQL function set.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Value type for UDF inputs and outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UdfValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for UdfValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Int(i) => write!(f, "{}", i),
            Self::Float(v) => write!(f, "{}", v),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Bytes(b) => write!(f, "0x{}", hex::encode(b)),
        }
    }
}

/// UDF argument type declaration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UdfType {
    Bool,
    Int,
    Float,
    String,
    Bytes,
    Any,
}

impl std::fmt::Display for UdfType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool => write!(f, "BOOLEAN"),
            Self::Int => write!(f, "INTEGER"),
            Self::Float => write!(f, "FLOAT"),
            Self::String => write!(f, "VARCHAR"),
            Self::Bytes => write!(f, "BYTES"),
            Self::Any => write!(f, "ANY"),
        }
    }
}

/// Scalar UDF function signature
pub type ScalarFn = Arc<dyn Fn(&[UdfValue]) -> Result<UdfValue> + Send + Sync>;

/// Aggregate UDF accumulator
pub trait AggregateAccumulator: Send + Sync {
    /// Add a value to the accumulator
    fn accumulate(&mut self, args: &[UdfValue]) -> Result<()>;
    /// Get the current accumulated result
    fn result(&self) -> Result<UdfValue>;
    /// Reset the accumulator for a new window
    fn reset(&mut self);
    /// Create a fresh copy of this accumulator
    fn clone_box(&self) -> Box<dyn AggregateAccumulator>;
}

/// Aggregate UDF factory
pub type AggregateFnFactory = Arc<dyn Fn() -> Box<dyn AggregateAccumulator> + Send + Sync>;

/// UDF definition
#[derive(Clone)]
pub struct UdfDefinition {
    /// Function name
    pub name: String,
    /// Description
    pub description: String,
    /// Argument types
    pub arg_types: Vec<UdfType>,
    /// Return type
    pub return_type: UdfType,
    /// Whether this is deterministic
    pub deterministic: bool,
    /// Function kind
    pub kind: UdfKind,
}

impl std::fmt::Debug for UdfDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfDefinition")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("arg_types", &self.arg_types)
            .field("return_type", &self.return_type)
            .field("kind", &self.kind)
            .finish()
    }
}

/// Kind of UDF
#[derive(Clone)]
pub enum UdfKind {
    Scalar(ScalarFn),
    Aggregate(AggregateFnFactory),
}

impl std::fmt::Debug for UdfKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scalar(_) => write!(f, "Scalar(<fn>)"),
            Self::Aggregate(_) => write!(f, "Aggregate(<factory>)"),
        }
    }
}

/// UDF Registry
pub struct UdfRegistry {
    functions: HashMap<String, UdfDefinition>,
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl UdfRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_builtins();
        registry
    }

    /// Register a scalar UDF
    pub fn register_scalar(
        &mut self,
        name: &str,
        description: &str,
        arg_types: Vec<UdfType>,
        return_type: UdfType,
        deterministic: bool,
        func: ScalarFn,
    ) -> Result<()> {
        let name_lower = name.to_lowercase();
        if self.functions.contains_key(&name_lower) {
            return Err(StreamlineError::Query(format!(
                "UDF '{}' already registered",
                name
            )));
        }

        self.functions.insert(
            name_lower,
            UdfDefinition {
                name: name.to_string(),
                description: description.to_string(),
                arg_types,
                return_type,
                deterministic,
                kind: UdfKind::Scalar(func),
            },
        );
        Ok(())
    }

    /// Register an aggregate UDF
    pub fn register_aggregate(
        &mut self,
        name: &str,
        description: &str,
        arg_types: Vec<UdfType>,
        return_type: UdfType,
        factory: AggregateFnFactory,
    ) -> Result<()> {
        let name_lower = name.to_lowercase();
        if self.functions.contains_key(&name_lower) {
            return Err(StreamlineError::Query(format!(
                "UDF '{}' already registered",
                name
            )));
        }

        self.functions.insert(
            name_lower,
            UdfDefinition {
                name: name.to_string(),
                description: description.to_string(),
                arg_types,
                return_type,
                deterministic: true,
                kind: UdfKind::Aggregate(factory),
            },
        );
        Ok(())
    }

    /// Look up a registered UDF
    pub fn get(&self, name: &str) -> Option<&UdfDefinition> {
        self.functions.get(&name.to_lowercase())
    }

    /// List all registered UDFs
    pub fn list(&self) -> Vec<&UdfDefinition> {
        self.functions.values().collect()
    }

    /// Unregister a UDF
    pub fn unregister(&mut self, name: &str) -> Result<()> {
        self.functions
            .remove(&name.to_lowercase())
            .map(|_| ())
            .ok_or_else(|| StreamlineError::Query(format!("UDF '{}' not found", name)))
    }

    /// Call a scalar UDF
    pub fn call_scalar(&self, name: &str, args: &[UdfValue]) -> Result<UdfValue> {
        let def = self
            .get(name)
            .ok_or_else(|| StreamlineError::Query(format!("Unknown function: {}", name)))?;

        match &def.kind {
            UdfKind::Scalar(func) => func(args),
            UdfKind::Aggregate(_) => Err(StreamlineError::Query(format!(
                "'{}' is an aggregate function, not scalar",
                name
            ))),
        }
    }

    fn register_builtins(&mut self) {
        // String functions
        let _ = self.register_scalar(
            "upper",
            "Convert string to uppercase",
            vec![UdfType::String],
            UdfType::String,
            true,
            Arc::new(|args| match args.first() {
                Some(UdfValue::String(s)) => Ok(UdfValue::String(s.to_uppercase())),
                Some(UdfValue::Null) => Ok(UdfValue::Null),
                _ => Err(StreamlineError::Query("UPPER expects a string".into())),
            }),
        );

        let _ = self.register_scalar(
            "lower",
            "Convert string to lowercase",
            vec![UdfType::String],
            UdfType::String,
            true,
            Arc::new(|args| match args.first() {
                Some(UdfValue::String(s)) => Ok(UdfValue::String(s.to_lowercase())),
                Some(UdfValue::Null) => Ok(UdfValue::Null),
                _ => Err(StreamlineError::Query("LOWER expects a string".into())),
            }),
        );

        let _ = self.register_scalar(
            "length",
            "Return string length",
            vec![UdfType::String],
            UdfType::Int,
            true,
            Arc::new(|args| match args.first() {
                Some(UdfValue::String(s)) => Ok(UdfValue::Int(s.len() as i64)),
                Some(UdfValue::Null) => Ok(UdfValue::Null),
                _ => Err(StreamlineError::Query("LENGTH expects a string".into())),
            }),
        );

        // Math functions
        let _ = self.register_scalar(
            "abs",
            "Absolute value",
            vec![UdfType::Float],
            UdfType::Float,
            true,
            Arc::new(|args| match args.first() {
                Some(UdfValue::Float(v)) => Ok(UdfValue::Float(v.abs())),
                Some(UdfValue::Int(v)) => Ok(UdfValue::Int(v.abs())),
                Some(UdfValue::Null) => Ok(UdfValue::Null),
                _ => Err(StreamlineError::Query("ABS expects a number".into())),
            }),
        );

        let _ = self.register_scalar(
            "coalesce",
            "Return first non-null argument",
            vec![UdfType::Any, UdfType::Any],
            UdfType::Any,
            true,
            Arc::new(|args| {
                for arg in args {
                    if !matches!(arg, UdfValue::Null) {
                        return Ok(arg.clone());
                    }
                }
                Ok(UdfValue::Null)
            }),
        );

        let _ = self.register_scalar(
            "now_ms",
            "Current time in milliseconds",
            vec![],
            UdfType::Int,
            false,
            Arc::new(|_args| Ok(UdfValue::Int(chrono::Utc::now().timestamp_millis()))),
        );
    }
}

/// Query explain plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplainPlan {
    pub sql: String,
    pub logical_plan: Vec<PlanNode>,
    pub estimated_cost: f64,
    pub uses_windowing: bool,
    pub uses_join: bool,
    pub uses_aggregation: bool,
    pub source_topics: Vec<String>,
}

/// A node in the explain plan tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNode {
    pub operator: String,
    pub description: String,
    pub estimated_rows: Option<u64>,
    pub children: Vec<PlanNode>,
}

impl std::fmt::Display for QueryExplainPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Query: {}", self.sql)?;
        writeln!(f, "Estimated cost: {:.2}", self.estimated_cost)?;
        writeln!(f, "Sources: {:?}", self.source_topics)?;
        if self.uses_windowing {
            writeln!(f, "  [Window]")?;
        }
        if self.uses_join {
            writeln!(f, "  [Join]")?;
        }
        if self.uses_aggregation {
            writeln!(f, "  [Aggregation]")?;
        }
        for node in &self.logical_plan {
            Self::fmt_node(f, node, 0)?;
        }
        Ok(())
    }
}

impl QueryExplainPlan {
    fn fmt_node(
        f: &mut std::fmt::Formatter<'_>,
        node: &PlanNode,
        depth: usize,
    ) -> std::fmt::Result {
        let indent = "  ".repeat(depth + 1);
        writeln!(f, "{}→ {} ({})", indent, node.operator, node.description)?;
        for child in &node.children {
            Self::fmt_node(f, child, depth + 1)?;
        }
        Ok(())
    }

    /// Generate an explain plan from a SQL query
    pub fn explain(sql: &str) -> Self {
        let sql_lower = sql.to_lowercase();
        let uses_windowing = sql_lower.contains("window")
            || sql_lower.contains("tumbling")
            || sql_lower.contains("hopping")
            || sql_lower.contains("session");
        let uses_join = sql_lower.contains(" join ");
        let uses_aggregation = sql_lower.contains("count(")
            || sql_lower.contains("sum(")
            || sql_lower.contains("avg(")
            || sql_lower.contains("min(")
            || sql_lower.contains("max(")
            || sql_lower.contains("group by");

        // Extract source topics from FROM clause
        let source_topics = Self::extract_topics(sql);

        let mut plan_nodes = Vec::new();

        // Build logical plan bottom-up
        plan_nodes.push(PlanNode {
            operator: "StreamScan".to_string(),
            description: format!("Scan from {:?}", source_topics),
            estimated_rows: None,
            children: Vec::new(),
        });

        if sql_lower.contains("where") {
            plan_nodes.push(PlanNode {
                operator: "Filter".to_string(),
                description: "Apply WHERE predicates".to_string(),
                estimated_rows: None,
                children: Vec::new(),
            });
        }

        if uses_join {
            plan_nodes.push(PlanNode {
                operator: "StreamJoin".to_string(),
                description: "Join streams".to_string(),
                estimated_rows: None,
                children: Vec::new(),
            });
        }

        if uses_aggregation {
            plan_nodes.push(PlanNode {
                operator: "Aggregate".to_string(),
                description: "Compute aggregations".to_string(),
                estimated_rows: None,
                children: Vec::new(),
            });
        }

        if uses_windowing {
            plan_nodes.push(PlanNode {
                operator: "Window".to_string(),
                description: "Apply windowing function".to_string(),
                estimated_rows: None,
                children: Vec::new(),
            });
        }

        plan_nodes.push(PlanNode {
            operator: "Project".to_string(),
            description: "Select output columns".to_string(),
            estimated_rows: None,
            children: Vec::new(),
        });

        let cost = plan_nodes.len() as f64 * 1.0
            + if uses_join { 5.0 } else { 0.0 }
            + if uses_windowing { 3.0 } else { 0.0 };

        Self {
            sql: sql.to_string(),
            logical_plan: plan_nodes,
            estimated_cost: cost,
            uses_windowing,
            uses_join,
            uses_aggregation,
            source_topics,
        }
    }

    fn extract_topics(sql: &str) -> Vec<String> {
        let lower = sql.to_lowercase();
        let mut topics = Vec::new();

        if let Some(from_pos) = lower.find("from ") {
            let after_from = &sql[from_pos + 5..];
            let end = after_from
                .find([' ', '\n', ')', ';'])
                .unwrap_or(after_from.len());
            let topic = after_from[..end].trim().to_string();
            if !topic.is_empty() {
                topics.push(topic);
            }
        }

        topics
    }
}

/// Late arrival policy for windowed queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LateArrivalPolicy {
    /// Drop late-arriving records
    Drop,
    /// Include in the most recent applicable window
    Include,
    /// Route to a dead-letter topic
    Redirect { dlq_topic: String },
    /// Allow up to a specified delay before dropping
    AllowedLateness { max_lateness_ms: u64 },
}

impl Default for LateArrivalPolicy {
    fn default() -> Self {
        Self::AllowedLateness {
            max_lateness_ms: 5000,
        }
    }
}

impl std::fmt::Display for LateArrivalPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drop => write!(f, "drop"),
            Self::Include => write!(f, "include"),
            Self::Redirect { dlq_topic } => write!(f, "redirect({})", dlq_topic),
            Self::AllowedLateness { max_lateness_ms } => {
                write!(f, "allowed-lateness({}ms)", max_lateness_ms)
            }
        }
    }
}

/// Watermark strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatermarkStrategy {
    /// Monotonically increasing timestamps
    MonotonicallyIncreasing,
    /// Bounded out-of-orderness with max delay
    BoundedOutOfOrder { max_delay_ms: u64 },
    /// Custom watermark from a field
    FromField { field_name: String, delay_ms: u64 },
}

impl Default for WatermarkStrategy {
    fn default() -> Self {
        Self::BoundedOutOfOrder { max_delay_ms: 1000 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udf_registry_builtins() {
        let registry = UdfRegistry::new();
        assert!(registry.get("upper").is_some());
        assert!(registry.get("lower").is_some());
        assert!(registry.get("length").is_some());
        assert!(registry.get("abs").is_some());
        assert!(registry.get("coalesce").is_some());
        assert!(registry.get("now_ms").is_some());
    }

    #[test]
    fn test_udf_call_upper() {
        let registry = UdfRegistry::new();
        let result = registry
            .call_scalar("upper", &[UdfValue::String("hello".into())])
            .unwrap();
        assert!(matches!(result, UdfValue::String(s) if s == "HELLO"));
    }

    #[test]
    fn test_udf_call_length() {
        let registry = UdfRegistry::new();
        let result = registry
            .call_scalar("length", &[UdfValue::String("test".into())])
            .unwrap();
        assert!(matches!(result, UdfValue::Int(4)));
    }

    #[test]
    fn test_udf_call_coalesce() {
        let registry = UdfRegistry::new();
        let result = registry
            .call_scalar(
                "coalesce",
                &[UdfValue::Null, UdfValue::String("fallback".into())],
            )
            .unwrap();
        assert!(matches!(result, UdfValue::String(s) if s == "fallback"));
    }

    #[test]
    fn test_register_custom_udf() {
        let mut registry = UdfRegistry::new();
        registry
            .register_scalar(
                "double",
                "Double a number",
                vec![UdfType::Int],
                UdfType::Int,
                true,
                Arc::new(|args| match args.first() {
                    Some(UdfValue::Int(v)) => Ok(UdfValue::Int(v * 2)),
                    _ => Err(StreamlineError::Query("expected int".into())),
                }),
            )
            .unwrap();

        let result = registry
            .call_scalar("double", &[UdfValue::Int(21)])
            .unwrap();
        assert!(matches!(result, UdfValue::Int(42)));
    }

    #[test]
    fn test_duplicate_registration_fails() {
        let mut registry = UdfRegistry::new();
        let result = registry.register_scalar(
            "upper",
            "Duplicate",
            vec![UdfType::String],
            UdfType::String,
            true,
            Arc::new(|_| Ok(UdfValue::Null)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_unregister_udf() {
        let mut registry = UdfRegistry::new();
        assert!(registry.unregister("upper").is_ok());
        assert!(registry.get("upper").is_none());
        assert!(registry.unregister("nonexistent").is_err());
    }

    #[test]
    fn test_explain_plan() {
        let plan = QueryExplainPlan::explain(
            "SELECT user_id, COUNT(*) FROM events WINDOW TUMBLING('1m') GROUP BY user_id",
        );
        assert!(plan.uses_windowing);
        assert!(plan.uses_aggregation);
        assert!(!plan.uses_join);
        assert_eq!(plan.source_topics, vec!["events"]);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_explain_join_query() {
        let plan = QueryExplainPlan::explain(
            "SELECT a.user_id, b.name FROM events a JOIN users b ON a.user_id = b.id",
        );
        assert!(plan.uses_join);
        assert!(!plan.uses_windowing);
    }

    #[test]
    fn test_late_arrival_policy_display() {
        assert_eq!(LateArrivalPolicy::Drop.to_string(), "drop");
        assert_eq!(LateArrivalPolicy::Include.to_string(), "include");
        assert_eq!(
            LateArrivalPolicy::default().to_string(),
            "allowed-lateness(5000ms)"
        );
    }

    #[test]
    fn test_udf_value_display() {
        assert_eq!(UdfValue::Null.to_string(), "NULL");
        assert_eq!(UdfValue::Int(42).to_string(), "42");
        assert_eq!(UdfValue::String("hi".into()).to_string(), "'hi'");
    }

    #[test]
    fn test_list_udfs() {
        let registry = UdfRegistry::new();
        let udfs = registry.list();
        assert!(udfs.len() >= 6); // builtins
    }
}
