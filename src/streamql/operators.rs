//! Stream operators for StreamlineQL
//!
//! Provides the building blocks for query execution.

use super::ast::{BinaryOperator, Expression, Literal, UnaryOperator};
use super::functions::{AggregateState, FunctionRegistry};
use super::types::{Row, Schema, Value};
use crate::error::{Result, StreamlineError};
use std::collections::HashMap;

/// Base trait for stream operators
pub trait StreamOperator: Send + Sync {
    /// Process a single row
    fn process(&mut self, row: Row) -> Result<Option<Row>>;

    /// Flush any buffered state (for aggregations)
    fn flush(&mut self) -> Result<Vec<Row>>;

    /// Get the output schema
    fn output_schema(&self) -> &Schema;
}

/// Filter operator - removes rows that don't match a predicate
pub struct FilterOperator {
    /// Filter expression
    predicate: Expression,
    /// Input schema
    schema: Schema,
    /// Function registry for evaluating expressions
    functions: FunctionRegistry,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(predicate: Expression, schema: Schema) -> Self {
        Self {
            predicate,
            schema,
            functions: FunctionRegistry::new(),
        }
    }

    fn evaluate_predicate(&self, row: &Row) -> Result<bool> {
        let value = evaluate_expression(&self.predicate, row, &self.schema, &self.functions)?;
        match value {
            Value::Boolean(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(StreamlineError::Query(
                "Filter predicate must evaluate to boolean".to_string(),
            )),
        }
    }
}

impl StreamOperator for FilterOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        if self.evaluate_predicate(&row)? {
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(Vec::new())
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Project operator - transforms rows by evaluating expressions
pub struct ProjectOperator {
    /// Projection expressions
    projections: Vec<(Expression, String)>,
    /// Input schema
    input_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Function registry
    functions: FunctionRegistry,
}

impl ProjectOperator {
    /// Create a new project operator
    pub fn new(
        projections: Vec<(Expression, String)>,
        input_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            projections,
            input_schema,
            output_schema,
            functions: FunctionRegistry::new(),
        }
    }

    fn project_row(&self, row: &Row) -> Result<Row> {
        let values: Result<Vec<Value>> = self
            .projections
            .iter()
            .map(|(expr, _)| evaluate_expression(expr, row, &self.input_schema, &self.functions))
            .collect();

        Ok(Row {
            values: values?,
            event_time: row.event_time,
        })
    }
}

impl StreamOperator for ProjectOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        Ok(Some(self.project_row(&row)?))
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(Vec::new())
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Aggregate operator - groups and aggregates rows
pub struct AggregateOperator {
    /// Group by expressions
    group_by: Vec<Expression>,
    /// Aggregate expressions (function name, argument, distinct)
    aggregates: Vec<(String, Expression, bool)>,
    /// Input schema
    input_schema: Schema,
    /// Output schema
    output_schema: Schema,
    /// Function registry
    functions: FunctionRegistry,
    /// Aggregate state by group key
    state: HashMap<Vec<Value>, Vec<AggregateState>>,
}

impl AggregateOperator {
    /// Create a new aggregate operator
    pub fn new(
        group_by: Vec<Expression>,
        aggregates: Vec<(String, Expression, bool)>,
        input_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input_schema,
            output_schema,
            functions: FunctionRegistry::new(),
            state: HashMap::new(),
        }
    }

    fn compute_group_key(&self, row: &Row) -> Result<Vec<Value>> {
        self.group_by
            .iter()
            .map(|expr| evaluate_expression(expr, row, &self.input_schema, &self.functions))
            .collect()
    }
}

impl StreamOperator for AggregateOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        let group_key = self.compute_group_key(&row)?;

        // Get or create aggregate state for this group
        let states = self.state.entry(group_key).or_insert_with(|| {
            self.aggregates
                .iter()
                .map(|(name, _, distinct)| {
                    AggregateState::new(name, *distinct).unwrap_or(AggregateState::Count {
                        count: 0,
                        distinct_values: None,
                    })
                })
                .collect()
        });

        // Update each aggregate
        for (i, (_, expr, _)) in self.aggregates.iter().enumerate() {
            let value = evaluate_expression(expr, &row, &self.input_schema, &self.functions)?;
            states[i].update(&value)?;
        }

        // Aggregates don't emit until flush
        Ok(None)
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        let mut results = Vec::new();

        for (group_key, states) in self.state.drain() {
            let mut values = group_key;
            for state in states {
                values.push(state.finalize());
            }
            results.push(Row::new(values));
        }

        Ok(results)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Limit operator - limits the number of output rows
pub struct LimitOperator {
    /// Maximum number of rows to emit
    limit: usize,
    /// Offset (number of rows to skip)
    offset: usize,
    /// Current row count
    count: usize,
    /// Schema
    schema: Schema,
}

impl LimitOperator {
    /// Create a new limit operator
    pub fn new(limit: usize, offset: usize, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            count: 0,
            schema,
        }
    }
}

impl StreamOperator for LimitOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        self.count += 1;

        if self.count <= self.offset {
            return Ok(None);
        }

        if self.count > self.offset + self.limit {
            return Ok(None);
        }

        Ok(Some(row))
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(Vec::new())
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Sort operator - sorts rows (requires buffering)
pub struct SortOperator {
    /// Sort keys (column index, ascending)
    sort_keys: Vec<(usize, bool)>,
    /// Buffer for sorting
    buffer: Vec<Row>,
    /// Schema
    schema: Schema,
}

impl SortOperator {
    /// Create a new sort operator
    pub fn new(sort_keys: Vec<(usize, bool)>, schema: Schema) -> Self {
        Self {
            sort_keys,
            buffer: Vec::new(),
            schema,
        }
    }
}

impl StreamOperator for SortOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        self.buffer.push(row);
        Ok(None)
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        // Sort the buffer
        self.buffer.sort_by(|a, b| {
            for (col_idx, ascending) in &self.sort_keys {
                let av = a.values.get(*col_idx);
                let bv = b.values.get(*col_idx);

                let cmp = match (av, bv) {
                    (Some(a), Some(b)) => a.partial_cmp(b),
                    (None, Some(_)) => Some(std::cmp::Ordering::Less),
                    (Some(_), None) => Some(std::cmp::Ordering::Greater),
                    (None, None) => Some(std::cmp::Ordering::Equal),
                };

                if let Some(ord) = cmp {
                    let ord = if *ascending { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(std::mem::take(&mut self.buffer))
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Distinct operator - removes duplicate rows
pub struct DistinctOperator {
    /// Seen values
    seen: std::collections::HashSet<Vec<u8>>,
    /// Schema
    schema: Schema,
}

impl DistinctOperator {
    /// Create a new distinct operator
    pub fn new(schema: Schema) -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            schema,
        }
    }

    fn row_key(row: &Row) -> Vec<u8> {
        // Simple hash key for the row
        let mut key = Vec::new();
        for value in &row.values {
            key.extend_from_slice(format!("{:?}", value).as_bytes());
            key.push(0);
        }
        key
    }
}

impl StreamOperator for DistinctOperator {
    fn process(&mut self, row: Row) -> Result<Option<Row>> {
        let key = Self::row_key(&row);
        if self.seen.insert(key) {
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(Vec::new())
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Evaluate an expression against a row
pub fn evaluate_expression(
    expr: &Expression,
    row: &Row,
    schema: &Schema,
    functions: &FunctionRegistry,
) -> Result<Value> {
    match expr {
        Expression::Literal(lit) => Ok(match lit {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Integer(i) => Value::Int64(*i),
            Literal::Float(f) => Value::Float64(*f),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Interval(d) => Value::Duration(d.as_millis() as i64),
        }),

        Expression::Column(col_ref) => {
            let col_name = &col_ref.column;
            let idx = schema
                .column_index(col_name)
                .ok_or_else(|| StreamlineError::Query(format!("Unknown column: {}", col_name)))?;
            row.values.get(idx).cloned().ok_or_else(|| {
                StreamlineError::Query(format!("Column index out of range: {}", idx))
            })
        }

        Expression::BinaryOp { left, op, right } => {
            let lval = evaluate_expression(left, row, schema, functions)?;
            let rval = evaluate_expression(right, row, schema, functions)?;
            evaluate_binary_op(&lval, op, &rval)
        }

        Expression::UnaryOp { op, expr } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            evaluate_unary_op(op, &val)
        }

        Expression::Function { name, args, .. } => {
            let arg_values: Result<Vec<Value>> = args
                .iter()
                .map(|arg| evaluate_expression(arg, row, schema, functions))
                .collect();
            let arg_values = arg_values?;

            if let Some(scalar_fn) = functions.get_scalar(name) {
                (scalar_fn.func)(&arg_values)
            } else {
                Err(StreamlineError::Query(format!(
                    "Unknown function: {}",
                    name
                )))
            }
        }

        Expression::IsNull { expr, negated } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            let is_null = val.is_null();
            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }

        Expression::In {
            expr,
            list,
            negated,
        } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            let list_values: Result<Vec<Value>> = list
                .iter()
                .map(|e| evaluate_expression(e, row, schema, functions))
                .collect();
            let list_values = list_values?;
            let found = list_values.iter().any(|v| v == &val);
            Ok(Value::Boolean(if *negated { !found } else { found }))
        }

        Expression::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            let low_val = evaluate_expression(low, row, schema, functions)?;
            let high_val = evaluate_expression(high, row, schema, functions)?;
            let in_range = val >= low_val && val <= high_val;
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }

        Expression::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            if let Value::String(s) = val {
                let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
                let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
                    .map_err(|e| StreamlineError::Query(format!("Invalid LIKE pattern: {}", e)))?;
                let matches = regex.is_match(&s);
                Ok(Value::Boolean(if *negated { !matches } else { matches }))
            } else {
                Ok(Value::Null)
            }
        }

        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let compare_val = if let Some(op) = operand {
                Some(evaluate_expression(op, row, schema, functions)?)
            } else {
                None
            };

            for (when_expr, then_expr) in when_clauses {
                let when_val = evaluate_expression(when_expr, row, schema, functions)?;

                let matches = if let Some(ref cmp) = compare_val {
                    when_val == *cmp
                } else {
                    when_val.as_bool().unwrap_or(false)
                };

                if matches {
                    return evaluate_expression(then_expr, row, schema, functions);
                }
            }

            if let Some(else_expr) = else_clause {
                evaluate_expression(else_expr, row, schema, functions)
            } else {
                Ok(Value::Null)
            }
        }

        Expression::Cast { expr, data_type } => {
            let val = evaluate_expression(expr, row, schema, functions)?;
            val.cast(data_type)
                .ok_or_else(|| StreamlineError::Query(format!("Cannot cast to {}", data_type)))
        }

        Expression::Nested(inner) => evaluate_expression(inner, row, schema, functions),

        _ => Err(StreamlineError::Query(format!(
            "Unsupported expression type: {:?}",
            expr
        ))),
    }
}

fn evaluate_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
    // Handle NULL propagation
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOperator::And => {
                if let Value::Boolean(false) = left {
                    return Ok(Value::Boolean(false));
                }
                if let Value::Boolean(false) = right {
                    return Ok(Value::Boolean(false));
                }
                Ok(Value::Null)
            }
            BinaryOperator::Or => {
                if let Value::Boolean(true) = left {
                    return Ok(Value::Boolean(true));
                }
                if let Value::Boolean(true) = right {
                    return Ok(Value::Boolean(true));
                }
                Ok(Value::Null)
            }
            _ => Ok(Value::Null),
        };
    }

    match op {
        // Arithmetic
        BinaryOperator::Add => match (left, right) {
            (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
            (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l + r)),
            (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64(*l as f64 + r)),
            (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(l + *r as f64)),
            _ => Err(StreamlineError::Query("Invalid operands for +".to_string())),
        },
        BinaryOperator::Subtract => match (left, right) {
            (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
            (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l - r)),
            (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64(*l as f64 - r)),
            (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(l - *r as f64)),
            _ => Err(StreamlineError::Query("Invalid operands for -".to_string())),
        },
        BinaryOperator::Multiply => match (left, right) {
            (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
            (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l * r)),
            (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64(*l as f64 * r)),
            (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(l * *r as f64)),
            _ => Err(StreamlineError::Query("Invalid operands for *".to_string())),
        },
        BinaryOperator::Divide => match (left, right) {
            (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Int64(l / r)),
            (Value::Float64(l), Value::Float64(r)) if *r != 0.0 => Ok(Value::Float64(l / r)),
            (Value::Int64(l), Value::Float64(r)) if *r != 0.0 => Ok(Value::Float64(*l as f64 / r)),
            (Value::Float64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Float64(l / *r as f64)),
            _ => Err(StreamlineError::Query(
                "Division by zero or invalid operands".to_string(),
            )),
        },
        BinaryOperator::Modulo => match (left, right) {
            (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Int64(l % r)),
            _ => Err(StreamlineError::Query("Invalid operands for %".to_string())),
        },

        // Comparison
        BinaryOperator::Equal => Ok(Value::Boolean(left == right)),
        BinaryOperator::NotEqual => Ok(Value::Boolean(left != right)),
        BinaryOperator::LessThan => Ok(Value::Boolean(left < right)),
        BinaryOperator::LessThanOrEqual => Ok(Value::Boolean(left <= right)),
        BinaryOperator::GreaterThan => Ok(Value::Boolean(left > right)),
        BinaryOperator::GreaterThanOrEqual => Ok(Value::Boolean(left >= right)),

        // Logical
        BinaryOperator::And => match (left.as_bool(), right.as_bool()) {
            (Some(l), Some(r)) => Ok(Value::Boolean(l && r)),
            _ => Err(StreamlineError::Query(
                "AND requires boolean operands".to_string(),
            )),
        },
        BinaryOperator::Or => match (left.as_bool(), right.as_bool()) {
            (Some(l), Some(r)) => Ok(Value::Boolean(l || r)),
            _ => Err(StreamlineError::Query(
                "OR requires boolean operands".to_string(),
            )),
        },

        // String
        BinaryOperator::Concat => {
            let l_str = left.as_string().unwrap_or_default();
            let r_str = right.as_string().unwrap_or_default();
            Ok(Value::String(l_str + &r_str))
        }

        _ => Err(StreamlineError::Query(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}

fn evaluate_unary_op(op: &UnaryOperator, value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }

    match op {
        UnaryOperator::Minus => match value {
            Value::Int64(n) => Ok(Value::Int64(-n)),
            Value::Float64(n) => Ok(Value::Float64(-n)),
            _ => Err(StreamlineError::Query(
                "Cannot negate non-numeric value".to_string(),
            )),
        },
        UnaryOperator::Not => match value.as_bool() {
            Some(b) => Ok(Value::Boolean(!b)),
            None => Err(StreamlineError::Query(
                "NOT requires boolean operand".to_string(),
            )),
        },
        UnaryOperator::BitwiseNot => match value {
            Value::Int64(n) => Ok(Value::Int64(!n)),
            _ => Err(StreamlineError::Query(
                "Bitwise NOT requires integer".to_string(),
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::Column;
    use super::super::types::DataType;
    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int64),
            Column::new("name", DataType::String),
            Column::new("value", DataType::Float64),
        ])
    }

    #[test]
    fn test_filter_operator() {
        let schema = test_schema();
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column(super::super::ast::ColumnRef::simple(
                "id",
            ))),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Integer(5))),
        };

        let mut filter = FilterOperator::new(predicate, schema);

        let row1 = Row::new(vec![
            Value::Int64(3),
            Value::String("test".to_string()),
            Value::Float64(1.0),
        ]);
        let row2 = Row::new(vec![
            Value::Int64(10),
            Value::String("test".to_string()),
            Value::Float64(2.0),
        ]);

        assert!(filter.process(row1).unwrap().is_none());
        assert!(filter.process(row2).unwrap().is_some());
    }

    #[test]
    fn test_limit_operator() {
        let schema = test_schema();
        let mut limit = LimitOperator::new(2, 1, schema);

        let results: Vec<_> = (0..5)
            .filter_map(|i| {
                let row = Row::new(vec![
                    Value::Int64(i),
                    Value::String("test".to_string()),
                    Value::Float64(i as f64),
                ]);
                limit.process(row).ok().flatten()
            })
            .collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].values[0], Value::Int64(1));
        assert_eq!(results[1].values[0], Value::Int64(2));
    }

    #[test]
    fn test_expression_evaluation() {
        let schema = test_schema();
        let functions = FunctionRegistry::new();

        let row = Row::new(vec![
            Value::Int64(42),
            Value::String("hello".to_string()),
            Value::Float64(std::f64::consts::PI),
        ]);

        // Test column reference
        let expr = Expression::Column(super::super::ast::ColumnRef::simple("id"));
        let result = evaluate_expression(&expr, &row, &schema, &functions).unwrap();
        assert_eq!(result, Value::Int64(42));

        // Test arithmetic
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column(super::super::ast::ColumnRef::simple(
                "id",
            ))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(Literal::Integer(2))),
        };
        let result = evaluate_expression(&expr, &row, &schema, &functions).unwrap();
        assert_eq!(result, Value::Int64(84));
    }
}
