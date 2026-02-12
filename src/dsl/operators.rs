//! Stream Processing Operators
//!
//! Defines the core operators for stream transformations including
//! filter, map, project, aggregate, and join operations.

use super::{DslContext, DslError, DslRecord, DslResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Base trait for all stream operators
pub trait Operator: Send + Sync {
    /// Process a single record
    fn process(&self, record: DslRecord, ctx: &DslContext) -> DslResult<Vec<DslRecord>>;

    /// Get operator name
    fn name(&self) -> &str;

    /// Check if this operator can produce multiple outputs
    fn is_multi_output(&self) -> bool {
        false
    }
}

/// Expression types for filtering and transformations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    /// Literal value
    Literal(serde_json::Value),
    /// Field reference
    Field(String),
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// Function call
    Function { name: String, args: Vec<Expression> },
    /// Conditional expression
    Conditional {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
    /// Variable reference
    Variable(String),
}

impl Expression {
    /// Evaluate the expression against a record
    pub fn evaluate(&self, record: &DslRecord, ctx: &DslContext) -> DslResult<serde_json::Value> {
        match self {
            Expression::Literal(v) => Ok(v.clone()),
            Expression::Field(name) => record
                .get(name)
                .cloned()
                .ok_or_else(|| DslError::FieldNotFound(name.clone())),
            Expression::BinaryOp { left, op, right } => {
                let lval = left.evaluate(record, ctx)?;
                let rval = right.evaluate(record, ctx)?;
                op.apply(&lval, &rval)
            }
            Expression::UnaryOp { op, expr } => {
                let val = expr.evaluate(record, ctx)?;
                op.apply(&val)
            }
            Expression::Function { name, args } => {
                let arg_values: Vec<serde_json::Value> = args
                    .iter()
                    .map(|a| a.evaluate(record, ctx))
                    .collect::<DslResult<_>>()?;
                evaluate_function(name, &arg_values)
            }
            Expression::Conditional {
                condition,
                then_expr,
                else_expr,
            } => {
                let cond = condition.evaluate(record, ctx)?;
                if cond.as_bool().unwrap_or(false) {
                    then_expr.evaluate(record, ctx)
                } else {
                    else_expr.evaluate(record, ctx)
                }
            }
            Expression::Variable(name) => ctx
                .get_var(name)
                .cloned()
                .ok_or_else(|| DslError::FieldNotFound(format!("variable: {}", name))),
        }
    }

    /// Create a literal expression
    pub fn literal(value: serde_json::Value) -> Self {
        Expression::Literal(value)
    }

    /// Create a field reference
    pub fn field(name: &str) -> Self {
        Expression::Field(name.to_string())
    }

    /// Create an equals comparison
    pub fn eq(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Eq,
            right: Box::new(right),
        }
    }

    /// Create a not equals comparison
    pub fn ne(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Ne,
            right: Box::new(right),
        }
    }

    /// Create a greater than comparison
    pub fn gt(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Gt,
            right: Box::new(right),
        }
    }

    /// Create a less than comparison
    pub fn lt(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Lt,
            right: Box::new(right),
        }
    }

    /// Create an AND expression
    pub fn and(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    /// Create an OR expression
    pub fn or(left: Expression, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Concat,
    Like,
    // Containment
    In,
    Contains,
}

impl BinaryOperator {
    /// Apply the operator to two values
    pub fn apply(
        &self,
        left: &serde_json::Value,
        right: &serde_json::Value,
    ) -> DslResult<serde_json::Value> {
        match self {
            BinaryOperator::Eq => Ok(serde_json::Value::Bool(left == right)),
            BinaryOperator::Ne => Ok(serde_json::Value::Bool(left != right)),
            BinaryOperator::Gt => compare_values(left, right, |a, b| a > b),
            BinaryOperator::Ge => compare_values(left, right, |a, b| a >= b),
            BinaryOperator::Lt => compare_values(left, right, |a, b| a < b),
            BinaryOperator::Le => compare_values(left, right, |a, b| a <= b),
            BinaryOperator::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(serde_json::Value::Bool(l && r))
            }
            BinaryOperator::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(serde_json::Value::Bool(l || r))
            }
            BinaryOperator::Add => arithmetic_op(left, right, |a, b| a + b),
            BinaryOperator::Sub => arithmetic_op(left, right, |a, b| a - b),
            BinaryOperator::Mul => arithmetic_op(left, right, |a, b| a * b),
            BinaryOperator::Div => {
                arithmetic_op(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 })
            }
            BinaryOperator::Mod => {
                let l = left.as_i64().unwrap_or(0);
                let r = right.as_i64().unwrap_or(1);
                Ok(serde_json::Value::Number((l % r).into()))
            }
            BinaryOperator::Concat => {
                let l = value_to_string(left);
                let r = value_to_string(right);
                Ok(serde_json::Value::String(format!("{}{}", l, r)))
            }
            BinaryOperator::Like => {
                let text = left.as_str().unwrap_or("");
                let pattern = right.as_str().unwrap_or("");
                Ok(serde_json::Value::Bool(like_match(text, pattern)))
            }
            BinaryOperator::In => {
                if let serde_json::Value::Array(arr) = right {
                    Ok(serde_json::Value::Bool(arr.contains(left)))
                } else {
                    Ok(serde_json::Value::Bool(false))
                }
            }
            BinaryOperator::Contains => {
                let text = left.as_str().unwrap_or("");
                let substr = right.as_str().unwrap_or("");
                Ok(serde_json::Value::Bool(text.contains(substr)))
            }
        }
    }
}

/// Unary operators
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

impl UnaryOperator {
    /// Apply the operator to a value
    pub fn apply(&self, value: &serde_json::Value) -> DslResult<serde_json::Value> {
        match self {
            UnaryOperator::Not => {
                let b = value.as_bool().unwrap_or(false);
                Ok(serde_json::Value::Bool(!b))
            }
            UnaryOperator::Neg => {
                if let Some(n) = value.as_f64() {
                    Ok(serde_json::json!(-n))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            UnaryOperator::IsNull => Ok(serde_json::Value::Bool(value.is_null())),
            UnaryOperator::IsNotNull => Ok(serde_json::Value::Bool(!value.is_null())),
        }
    }
}

/// Filter operator - passes or blocks records based on a condition
#[derive(Debug, Clone)]
pub struct FilterOperator {
    pub condition: Expression,
}

impl FilterOperator {
    pub fn new(condition: Expression) -> Self {
        Self { condition }
    }
}

impl Operator for FilterOperator {
    fn process(&self, record: DslRecord, ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        let result = self.condition.evaluate(&record, ctx)?;
        if result.as_bool().unwrap_or(false) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    fn name(&self) -> &str {
        "filter"
    }
}

/// Filter operation specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOp {
    pub condition: Expression,
}

/// Map operator - transforms records
#[derive(Debug, Clone)]
pub struct MapOperator {
    pub transformations: Vec<(String, Expression)>,
}

impl MapOperator {
    pub fn new(transformations: Vec<(String, Expression)>) -> Self {
        Self { transformations }
    }
}

impl Operator for MapOperator {
    fn process(&self, mut record: DslRecord, ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        for (field, expr) in &self.transformations {
            let value = expr.evaluate(&record, ctx)?;
            record.set(field, value);
        }
        Ok(vec![record])
    }

    fn name(&self) -> &str {
        "map"
    }
}

/// Map operation specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapOp {
    pub transformations: Vec<(String, Expression)>,
}

/// Project operator - selects specific fields
#[derive(Debug, Clone)]
pub struct ProjectOperator {
    pub fields: Vec<String>,
    pub aliases: HashMap<String, String>,
}

impl ProjectOperator {
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            aliases: HashMap::new(),
        }
    }

    pub fn with_aliases(fields: Vec<String>, aliases: HashMap<String, String>) -> Self {
        Self { fields, aliases }
    }
}

impl Operator for ProjectOperator {
    fn process(&self, record: DslRecord, _ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        let mut new_value = serde_json::Map::new();

        if let serde_json::Value::Object(map) = &record.value {
            for field in &self.fields {
                if let Some(v) = map.get(field) {
                    let output_name = self.aliases.get(field).unwrap_or(field);
                    new_value.insert(output_name.clone(), v.clone());
                }
            }
        }

        Ok(vec![DslRecord {
            key: record.key,
            value: serde_json::Value::Object(new_value),
            timestamp: record.timestamp,
            headers: record.headers,
            source_topic: record.source_topic,
            source_partition: record.source_partition,
            source_offset: record.source_offset,
        }])
    }

    fn name(&self) -> &str {
        "project"
    }
}

/// Project operation specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectOp {
    pub fields: Vec<String>,
    pub aliases: HashMap<String, String>,
}

/// Aggregate functions
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    CountDistinct,
}

/// Aggregate operator - performs aggregations over windows
#[derive(Debug, Clone)]
pub struct AggregateOperator {
    pub aggregations: Vec<(String, AggregateFunction, Option<String>)>,
    pub group_by: Vec<String>,
}

impl AggregateOperator {
    pub fn new(
        aggregations: Vec<(String, AggregateFunction, Option<String>)>,
        group_by: Vec<String>,
    ) -> Self {
        Self {
            aggregations,
            group_by,
        }
    }

    /// Aggregate a batch of records
    pub fn aggregate_batch(&self, records: &[DslRecord]) -> DslResult<Vec<DslRecord>> {
        if records.is_empty() {
            return Ok(vec![]);
        }

        // Group records
        let mut groups: HashMap<String, Vec<&DslRecord>> = HashMap::new();

        for record in records {
            let key = self.compute_group_key(record);
            groups.entry(key).or_default().push(record);
        }

        // Aggregate each group
        let mut results = Vec::new();
        for (_group_key, group_records) in groups {
            let mut result_value = serde_json::Map::new();

            // Add group-by fields
            if let Some(first) = group_records.first() {
                for field in &self.group_by {
                    if let Some(v) = first.get(field) {
                        result_value.insert(field.clone(), v.clone());
                    }
                }
            }

            // Compute aggregations
            for (output_name, func, input_field) in &self.aggregations {
                let agg_value =
                    self.compute_aggregation(*func, input_field.as_deref(), &group_records)?;
                result_value.insert(output_name.clone(), agg_value);
            }

            let first = group_records.first().ok_or_else(|| {
                DslError::ExecutionError("Empty group in aggregation".to_string())
            })?;
            results.push(DslRecord {
                key: None,
                value: serde_json::Value::Object(result_value),
                timestamp: chrono::Utc::now().timestamp_millis(),
                headers: HashMap::new(),
                source_topic: first.source_topic.clone(),
                source_partition: 0,
                source_offset: 0,
            });
        }

        Ok(results)
    }

    fn compute_group_key(&self, record: &DslRecord) -> String {
        if self.group_by.is_empty() {
            return "".to_string();
        }

        self.group_by
            .iter()
            .map(|field| record.get(field).map(value_to_string).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("|")
    }

    fn compute_aggregation(
        &self,
        func: AggregateFunction,
        field: Option<&str>,
        records: &[&DslRecord],
    ) -> DslResult<serde_json::Value> {
        match func {
            AggregateFunction::Count => Ok(serde_json::json!(records.len())),
            AggregateFunction::Sum => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("SUM requires a field".into()))?;
                let sum: f64 = records.iter().filter_map(|r| r.get_f64(field)).sum();
                Ok(serde_json::json!(sum))
            }
            AggregateFunction::Avg => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("AVG requires a field".into()))?;
                let values: Vec<f64> = records.iter().filter_map(|r| r.get_f64(field)).collect();
                if values.is_empty() {
                    Ok(serde_json::Value::Null)
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    Ok(serde_json::json!(avg))
                }
            }
            AggregateFunction::Min => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("MIN requires a field".into()))?;
                let min = records
                    .iter()
                    .filter_map(|r| r.get_f64(field))
                    .fold(f64::INFINITY, f64::min);
                if min.is_infinite() {
                    Ok(serde_json::Value::Null)
                } else {
                    Ok(serde_json::json!(min))
                }
            }
            AggregateFunction::Max => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("MAX requires a field".into()))?;
                let max = records
                    .iter()
                    .filter_map(|r| r.get_f64(field))
                    .fold(f64::NEG_INFINITY, f64::max);
                if max.is_infinite() {
                    Ok(serde_json::Value::Null)
                } else {
                    Ok(serde_json::json!(max))
                }
            }
            AggregateFunction::First => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("FIRST requires a field".into()))?;
                Ok(records
                    .first()
                    .and_then(|r| r.get(field))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null))
            }
            AggregateFunction::Last => {
                let field = field
                    .ok_or_else(|| DslError::InvalidOperator("LAST requires a field".into()))?;
                Ok(records
                    .last()
                    .and_then(|r| r.get(field))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null))
            }
            AggregateFunction::CountDistinct => {
                let field = field.ok_or_else(|| {
                    DslError::InvalidOperator("COUNT_DISTINCT requires a field".into())
                })?;
                let distinct: std::collections::HashSet<String> = records
                    .iter()
                    .filter_map(|r| r.get(field))
                    .map(value_to_string)
                    .collect();
                Ok(serde_json::json!(distinct.len()))
            }
        }
    }
}

impl Operator for AggregateOperator {
    fn process(&self, record: DslRecord, _ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        // Single record aggregation - just pass through
        // Real aggregation happens in aggregate_batch
        Ok(vec![record])
    }

    fn name(&self) -> &str {
        "aggregate"
    }

    fn is_multi_output(&self) -> bool {
        true
    }
}

/// Aggregate operation specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateOp {
    pub aggregations: Vec<(String, AggregateFunction, Option<String>)>,
    pub group_by: Vec<String>,
}

/// Join types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// Join operator - joins two streams
#[derive(Debug, Clone)]
pub struct JoinOperator {
    pub join_type: JoinType,
    pub left_key: String,
    pub right_key: String,
    pub right_topic: String,
    pub window_ms: u64,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        left_key: &str,
        right_key: &str,
        right_topic: &str,
        window_ms: u64,
    ) -> Self {
        Self {
            join_type,
            left_key: left_key.to_string(),
            right_key: right_key.to_string(),
            right_topic: right_topic.to_string(),
            window_ms,
        }
    }

    /// Join two records
    pub fn join_records(
        &self,
        left: &DslRecord,
        right: Option<&DslRecord>,
    ) -> DslResult<Option<DslRecord>> {
        match (self.join_type, right) {
            (JoinType::Inner, Some(r)) => {
                let merged = self.merge_records(left, r)?;
                Ok(Some(merged))
            }
            (JoinType::Inner, None) => Ok(None),
            (JoinType::LeftOuter, right_opt) => {
                if let Some(r) = right_opt {
                    let merged = self.merge_records(left, r)?;
                    Ok(Some(merged))
                } else {
                    Ok(Some(left.clone()))
                }
            }
            (JoinType::RightOuter, Some(r)) => {
                let merged = self.merge_records(left, r)?;
                Ok(Some(merged))
            }
            (JoinType::RightOuter, None) => Ok(None),
            (JoinType::FullOuter, right_opt) => {
                if let Some(r) = right_opt {
                    let merged = self.merge_records(left, r)?;
                    Ok(Some(merged))
                } else {
                    Ok(Some(left.clone()))
                }
            }
        }
    }

    fn merge_records(&self, left: &DslRecord, right: &DslRecord) -> DslResult<DslRecord> {
        let mut merged = serde_json::Map::new();

        // Add left fields with prefix
        if let serde_json::Value::Object(map) = &left.value {
            for (k, v) in map {
                merged.insert(format!("left_{}", k), v.clone());
            }
        }

        // Add right fields with prefix
        if let serde_json::Value::Object(map) = &right.value {
            for (k, v) in map {
                merged.insert(format!("right_{}", k), v.clone());
            }
        }

        Ok(DslRecord {
            key: left.key.clone(),
            value: serde_json::Value::Object(merged),
            timestamp: left.timestamp.max(right.timestamp),
            headers: left.headers.clone(),
            source_topic: left.source_topic.clone(),
            source_partition: left.source_partition,
            source_offset: left.source_offset,
        })
    }
}

impl Operator for JoinOperator {
    fn process(&self, record: DslRecord, _ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        // Join requires state management, handled at pipeline level
        Ok(vec![record])
    }

    fn name(&self) -> &str {
        "join"
    }
}

/// Join operation specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOp {
    pub join_type: JoinType,
    pub left_key: String,
    pub right_key: String,
    pub right_topic: String,
    pub window_ms: u64,
}

/// Chain of operators to process records through
pub struct OperatorChain {
    operators: Vec<Arc<dyn Operator>>,
}

impl OperatorChain {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    pub fn add<O: Operator + 'static>(&mut self, op: O) {
        self.operators.push(Arc::new(op));
    }

    pub fn process(&self, record: DslRecord, ctx: &DslContext) -> DslResult<Vec<DslRecord>> {
        let mut records = vec![record];

        for op in &self.operators {
            let mut next_records = Vec::new();
            for r in records {
                let results = op.process(r, ctx)?;
                next_records.extend(results);
            }
            records = next_records;

            if records.is_empty() {
                break;
            }
        }

        Ok(records)
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }
}

impl Default for OperatorChain {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn compare_values<F>(
    left: &serde_json::Value,
    right: &serde_json::Value,
    cmp: F,
) -> DslResult<serde_json::Value>
where
    F: Fn(f64, f64) -> bool,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => Ok(serde_json::Value::Bool(cmp(l, r))),
        _ => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(serde_json::Value::Bool(cmp(l.len() as f64, r.len() as f64))),
            _ => Ok(serde_json::Value::Bool(false)),
        },
    }
}

fn arithmetic_op<F>(
    left: &serde_json::Value,
    right: &serde_json::Value,
    op: F,
) -> DslResult<serde_json::Value>
where
    F: Fn(f64, f64) -> f64,
{
    let l = left.as_f64().unwrap_or(0.0);
    let r = right.as_f64().unwrap_or(0.0);
    Ok(serde_json::json!(op(l, r)))
}

fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn like_match(text: &str, pattern: &str) -> bool {
    // Simple LIKE pattern matching (% = any, _ = single char)
    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
    regex::Regex::new(&format!("^{}$", regex_pattern))
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

fn evaluate_function(name: &str, args: &[serde_json::Value]) -> DslResult<serde_json::Value> {
    match name.to_uppercase().as_str() {
        "UPPER" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(serde_json::Value::String(s.to_uppercase()))
        }
        "LOWER" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(serde_json::Value::String(s.to_lowercase()))
        }
        "LENGTH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(serde_json::json!(s.len()))
        }
        "TRIM" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(serde_json::Value::String(s.trim().to_string()))
        }
        "SUBSTRING" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let start = args.get(1).and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            let len = args.get(2).and_then(|v| v.as_u64());
            let result = if let Some(l) = len {
                s.chars().skip(start).take(l as usize).collect()
            } else {
                s.chars().skip(start).collect()
            };
            Ok(serde_json::Value::String(result))
        }
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(serde_json::Value::Null)
        }
        "ABS" => {
            let n = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(serde_json::json!(n.abs()))
        }
        "ROUND" => {
            let n = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let factor = 10_f64.powi(precision as i32);
            Ok(serde_json::json!((n * factor).round() / factor))
        }
        "NOW" => Ok(serde_json::json!(chrono::Utc::now().timestamp_millis())),
        "CONCAT" => {
            let result: String = args.iter().map(value_to_string).collect();
            Ok(serde_json::Value::String(result))
        }
        _ => Err(DslError::InvalidOperator(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_operator() {
        let filter = FilterOperator::new(Expression::eq(
            Expression::field("type"),
            Expression::literal(serde_json::json!("click")),
        ));

        let ctx = DslContext::new();

        // Should pass
        let record1 = DslRecord::new(serde_json::json!({"type": "click", "user": "a"}), "test");
        let result1 = filter.process(record1, &ctx).unwrap();
        assert_eq!(result1.len(), 1);

        // Should filter out
        let record2 = DslRecord::new(serde_json::json!({"type": "view", "user": "b"}), "test");
        let result2 = filter.process(record2, &ctx).unwrap();
        assert_eq!(result2.len(), 0);
    }

    #[test]
    fn test_map_operator() {
        let map = MapOperator::new(vec![(
            "upper_name".to_string(),
            Expression::Function {
                name: "UPPER".to_string(),
                args: vec![Expression::field("name")],
            },
        )]);

        let ctx = DslContext::new();
        let record = DslRecord::new(serde_json::json!({"name": "alice"}), "test");
        let result = map.process(record, &ctx).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_str("upper_name"), Some("ALICE"));
    }

    #[test]
    fn test_project_operator() {
        let project = ProjectOperator::new(vec!["user_id".to_string(), "count".to_string()]);

        let ctx = DslContext::new();
        let record = DslRecord::new(
            serde_json::json!({"user_id": "u1", "count": 5, "extra": "data"}),
            "test",
        );
        let result = project.process(record, &ctx).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_str("user_id"), Some("u1"));
        assert_eq!(result[0].get_i64("count"), Some(5));
        assert_eq!(result[0].get_str("extra"), None);
    }

    #[test]
    fn test_aggregate_operator() {
        let agg = AggregateOperator::new(
            vec![
                ("total".to_string(), AggregateFunction::Count, None),
                (
                    "sum_value".to_string(),
                    AggregateFunction::Sum,
                    Some("value".to_string()),
                ),
            ],
            vec!["group".to_string()],
        );

        let records = vec![
            DslRecord::new(serde_json::json!({"group": "a", "value": 10}), "test"),
            DslRecord::new(serde_json::json!({"group": "a", "value": 20}), "test"),
            DslRecord::new(serde_json::json!({"group": "b", "value": 5}), "test"),
        ];

        let result = agg.aggregate_batch(&records).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_operator_chain() {
        let mut chain = OperatorChain::new();
        chain.add(FilterOperator::new(Expression::gt(
            Expression::field("value"),
            Expression::literal(serde_json::json!(5)),
        )));
        chain.add(ProjectOperator::new(vec!["value".to_string()]));

        let ctx = DslContext::new();
        let record = DslRecord::new(serde_json::json!({"value": 10, "extra": "data"}), "test");
        let result = chain.process(record, &ctx).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_i64("value"), Some(10));
        assert_eq!(result[0].get_str("extra"), None);
    }

    #[test]
    fn test_expression_evaluation() {
        let expr = Expression::and(
            Expression::gt(
                Expression::field("age"),
                Expression::literal(serde_json::json!(18)),
            ),
            Expression::eq(
                Expression::field("status"),
                Expression::literal(serde_json::json!("active")),
            ),
        );

        let ctx = DslContext::new();
        let record = DslRecord::new(serde_json::json!({"age": 25, "status": "active"}), "test");

        let result = expr.evaluate(&record, &ctx).unwrap();
        assert_eq!(result.as_bool(), Some(true));
    }
}
