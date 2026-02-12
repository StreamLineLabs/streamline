//! Query executor for StreamlineQL
//!
//! Executes physical query plans against streaming data.

use super::ast::{Expression, JoinType};
use super::functions::FunctionRegistry;
use super::operators::{
    evaluate_expression, AggregateOperator, DistinctOperator, FilterOperator, LimitOperator,
    ProjectOperator, SortOperator, StreamOperator,
};
use super::planner::{LogicalPlan, PhysicalPlan};
#[cfg(test)]
use super::types::{Column, DataType};
use super::types::{Row, Schema, Value};
use super::window::{WindowManager, WindowType};
use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Query execution result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result schema
    pub schema: Schema,
    /// Result rows
    pub rows: Vec<Row>,
    /// Execution statistics
    pub stats: ExecutionStats,
}

impl QueryResult {
    /// Create a new query result
    pub fn new(schema: Schema, rows: Vec<Row>) -> Self {
        Self {
            schema,
            rows,
            stats: ExecutionStats::default(),
        }
    }

    /// Create an empty result with schema
    pub fn empty(schema: Schema) -> Self {
        Self::new(schema, Vec::new())
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Number of rows scanned
    pub rows_scanned: u64,
    /// Number of rows filtered
    pub rows_filtered: u64,
    /// Number of rows output
    pub rows_output: u64,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Memory used in bytes
    pub memory_bytes: u64,
}

/// Streaming query result
pub struct StreamResult {
    /// Result schema
    pub schema: Schema,
    /// Receiver for streaming rows
    pub receiver: mpsc::Receiver<Result<Row>>,
}

impl StreamResult {
    /// Create a new streaming result
    pub fn new(schema: Schema, receiver: mpsc::Receiver<Result<Row>>) -> Self {
        Self { schema, receiver }
    }
}

/// Query executor
pub struct QueryExecutor {
    /// Topic manager for accessing data
    topic_manager: Arc<TopicManager>,
    /// Function registry
    functions: Arc<FunctionRegistry>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager,
            functions: Arc::new(FunctionRegistry::new()),
        }
    }

    /// Execute a physical plan
    pub async fn execute(&self, plan: PhysicalPlan) -> Result<QueryResult> {
        let start = std::time::Instant::now();

        let (schema, rows) = self.execute_plan(&plan.logical).await?;

        let stats = ExecutionStats {
            rows_output: rows.len() as u64,
            execution_time_us: start.elapsed().as_micros() as u64,
            ..Default::default()
        };

        Ok(QueryResult {
            schema,
            rows,
            stats,
        })
    }

    /// Execute a logical plan
    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<(Schema, Vec<Row>)> {
        match plan {
            LogicalPlan::Scan { source, schema, .. } => {
                let rows = self.scan_topic(source, schema).await?;
                Ok((schema.clone(), rows))
            }

            LogicalPlan::Filter { input, predicate } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                let mut operator = FilterOperator::new(predicate.clone(), input_schema.clone());
                let filtered: Vec<Row> = rows
                    .into_iter()
                    .filter_map(|row| operator.process(row).ok().flatten())
                    .collect();

                Ok((input_schema, filtered))
            }

            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                let mut operator =
                    ProjectOperator::new(expressions.clone(), input_schema, schema.clone());
                let projected: Vec<Row> = rows
                    .into_iter()
                    .filter_map(|row| operator.process(row).ok().flatten())
                    .collect();

                Ok((schema.clone(), projected))
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                // Convert aggregates to operator format (without alias)
                let agg_exprs: Vec<(String, Expression, bool)> = aggregates
                    .iter()
                    .map(|(name, expr, distinct, _alias)| (name.clone(), expr.clone(), *distinct))
                    .collect();

                let mut operator = AggregateOperator::new(
                    group_by.clone(),
                    agg_exprs,
                    input_schema,
                    schema.clone(),
                );

                for row in rows {
                    operator.process(row)?;
                }

                let result_rows = operator.flush()?;
                Ok((schema.clone(), result_rows))
            }

            LogicalPlan::Window {
                input,
                window_type,
                partition_by,
                group_by,
                aggregates,
                schema,
            } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                let window_type = convert_ast_window_type(window_type)?;
                let result_rows = self.execute_window(
                    rows,
                    window_type,
                    partition_by.as_deref().unwrap_or(&[]),
                    group_by,
                    aggregates,
                    &input_schema,
                )?;

                Ok((schema.clone(), result_rows))
            }

            LogicalPlan::Sort { input, order_by } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                // Convert expressions to column indices for sorting
                let sort_keys: Vec<(usize, bool)> = order_by
                    .iter()
                    .filter_map(|(expr, ascending, _nulls_first)| {
                        if let Expression::Column(col_ref) = expr {
                            input_schema
                                .column_index(&col_ref.column)
                                .map(|idx| (idx, *ascending))
                        } else {
                            None
                        }
                    })
                    .collect();

                let mut operator = SortOperator::new(sort_keys, input_schema.clone());

                for row in rows {
                    operator.process(row)?;
                }

                let sorted_rows = operator.flush()?;
                Ok((input_schema, sorted_rows))
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                let mut operator = LimitOperator::new(*limit, *offset, input_schema.clone());
                let limited: Vec<Row> = rows
                    .into_iter()
                    .filter_map(|row| operator.process(row).ok().flatten())
                    .collect();

                Ok((input_schema, limited))
            }

            LogicalPlan::Distinct { input } => {
                let (input_schema, rows) = Box::pin(self.execute_plan(input)).await?;

                let mut operator = DistinctOperator::new(input_schema.clone());
                let distinct: Vec<Row> = rows
                    .into_iter()
                    .filter_map(|row| operator.process(row).ok().flatten())
                    .collect();

                Ok((input_schema, distinct))
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let (left_schema, left_rows) = Box::pin(self.execute_plan(left)).await?;
                let (right_schema, right_rows) = Box::pin(self.execute_plan(right)).await?;

                let result_rows = self.execute_join(
                    left_rows,
                    right_rows,
                    join_type,
                    condition.as_ref(),
                    &left_schema,
                    &right_schema,
                )?;

                Ok((schema.clone(), result_rows))
            }

            LogicalPlan::Union { left, right, all } => {
                let (left_schema, left_rows) = Box::pin(self.execute_plan(left)).await?;
                let (_, right_rows) = Box::pin(self.execute_plan(right)).await?;

                let mut all_rows = left_rows;
                all_rows.extend(right_rows);

                if !all {
                    // Remove duplicates
                    let mut seen = std::collections::HashSet::new();
                    all_rows.retain(|row| {
                        let key = format!("{:?}", row.values);
                        seen.insert(key)
                    });
                }

                Ok((left_schema, all_rows))
            }
        }
    }

    /// Scan a topic for records
    async fn scan_topic(&self, topic_name: &str, _schema: &Schema) -> Result<Vec<Row>> {
        let mut rows = Vec::new();

        // Get topic metadata to find partition count
        let metadata = self.topic_manager.get_topic_metadata(topic_name)?;
        let partition_count = metadata.num_partitions;

        // Read from all partitions
        for partition_id in 0..partition_count {
            // Get offset range for this partition
            let start_offset = self
                .topic_manager
                .earliest_offset(topic_name, partition_id)?;
            let end_offset = self.topic_manager.latest_offset(topic_name, partition_id)?;

            // Calculate max records to read
            let max_records = (end_offset - start_offset) as usize;
            if max_records == 0 {
                continue;
            }

            // Read all records from this partition
            let records =
                self.topic_manager
                    .read(topic_name, partition_id, start_offset, max_records)?;

            for record in records {
                // Convert record to row
                let row = Row::with_event_time(
                    vec![
                        Value::Int64(record.offset),
                        Value::Timestamp(record.timestamp),
                        Value::Binary(record.key.as_ref().map(|k| k.to_vec()).unwrap_or_default()),
                        Value::Binary(record.value.to_vec()),
                        Value::Int64(partition_id as i64),
                    ],
                    record.timestamp,
                );
                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Execute a window operation
    fn execute_window(
        &self,
        rows: Vec<Row>,
        window_type: WindowType,
        partition_by: &[Expression],
        group_by: &[Expression],
        aggregates: &[(String, Expression, bool, String)],
        schema: &Schema,
    ) -> Result<Vec<Row>> {
        if partition_by.is_empty() {
            // Single partition
            self.execute_single_window(rows, window_type, group_by, aggregates, schema)
        } else {
            // Partitioned windows
            let mut partitions: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();

            for row in rows {
                let partition_key: Vec<Value> = partition_by
                    .iter()
                    .map(|expr| evaluate_expression(expr, &row, schema, &self.functions))
                    .collect::<Result<_>>()?;

                partitions.entry(partition_key).or_default().push(row);
            }

            let mut result_rows = Vec::new();
            for (_, partition_rows) in partitions {
                let window_results = self.execute_single_window(
                    partition_rows,
                    window_type.clone(),
                    group_by,
                    aggregates,
                    schema,
                )?;
                result_rows.extend(window_results);
            }

            Ok(result_rows)
        }
    }

    fn execute_single_window(
        &self,
        rows: Vec<Row>,
        window_type: WindowType,
        group_by: &[Expression],
        aggregates: &[(String, Expression, bool, String)],
        schema: &Schema,
    ) -> Result<Vec<Row>> {
        let mut manager = WindowManager::new(window_type);

        for row in &rows {
            let event_time = row.event_time.unwrap_or(0);
            manager.add_event(event_time, row.clone());
            manager.update_watermark(event_time);
        }

        // Emit all windows at the end
        let windows = manager.emit_all_windows();
        let mut result_rows = Vec::new();

        for window in windows {
            if window.rows.is_empty() {
                continue;
            }

            // Group rows within the window
            let mut groups: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();

            for row in &window.rows {
                let group_key: Vec<Value> = group_by
                    .iter()
                    .map(|expr| evaluate_expression(expr, row, schema, &self.functions))
                    .collect::<Result<_>>()?;

                groups.entry(group_key).or_default().push(row);
            }

            // Compute aggregates for each group
            for (group_key, group_rows) in groups {
                let mut values = vec![
                    Value::Timestamp(window.start_time),
                    Value::Timestamp(window.end_time),
                ];
                values.extend(group_key);

                // Compute each aggregate
                for (func_name, arg_expr, _distinct, _alias) in aggregates {
                    let agg_value =
                        self.compute_aggregate(func_name, arg_expr, &group_rows, schema)?;
                    values.push(agg_value);
                }

                result_rows.push(Row::with_event_time(values, window.end_time));
            }
        }

        Ok(result_rows)
    }

    fn compute_aggregate(
        &self,
        func_name: &str,
        arg_expr: &Expression,
        rows: &[&Row],
        schema: &Schema,
    ) -> Result<Value> {
        let values: Vec<Value> = rows
            .iter()
            .map(|row| evaluate_expression(arg_expr, row, schema, &self.functions))
            .collect::<Result<_>>()?;

        match func_name.to_uppercase().as_str() {
            "COUNT" => {
                // COUNT(*) is represented by a Literal::Integer(1) or empty function args
                // For COUNT(column), count non-null values
                let is_count_star =
                    matches!(
                        arg_expr,
                        Expression::Literal(super::ast::Literal::Integer(1))
                    ) || matches!(arg_expr, Expression::Literal(super::ast::Literal::Null));

                if is_count_star {
                    Ok(Value::Int64(rows.len() as i64))
                } else {
                    let count = values.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::Int64(count as i64))
                }
            }
            "SUM" => {
                let sum: f64 = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int64(n) => Some(*n as f64),
                        Value::Float64(n) => Some(*n),
                        _ => None,
                    })
                    .sum();
                Ok(Value::Float64(sum))
            }
            "AVG" => {
                let nums: Vec<f64> = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Int64(n) => Some(*n as f64),
                        Value::Float64(n) => Some(*n),
                        _ => None,
                    })
                    .collect();
                if nums.is_empty() {
                    Ok(Value::Null)
                } else {
                    let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                    Ok(Value::Float64(avg))
                }
            }
            "MIN" => {
                let min = values
                    .iter()
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(min)
            }
            "MAX" => {
                let max = values
                    .iter()
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(max)
            }
            "FIRST" => Ok(values.first().cloned().unwrap_or(Value::Null)),
            "LAST" => Ok(values.last().cloned().unwrap_or(Value::Null)),
            _ => Err(StreamlineError::Query(format!(
                "Unknown aggregate function: {}",
                func_name
            ))),
        }
    }

    /// Execute a join operation
    fn execute_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        join_type: &JoinType,
        condition: Option<&Expression>,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Vec<Row>> {
        let mut result = Vec::new();

        // Create combined schema for condition evaluation
        let combined_schema = Schema {
            columns: left_schema
                .columns
                .iter()
                .chain(right_schema.columns.iter())
                .cloned()
                .collect(),
        };

        match join_type {
            JoinType::Inner => {
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .chain(right_row.values.iter())
                            .cloned()
                            .collect();

                        let matches = if let Some(cond) = condition {
                            let combined_row = Row::new(combined_values.clone());
                            match evaluate_expression(
                                cond,
                                &combined_row,
                                &combined_schema,
                                &self.functions,
                            )? {
                                Value::Boolean(b) => b,
                                _ => false,
                            }
                        } else {
                            true // Cross join
                        };

                        if matches {
                            result.push(Row::new(combined_values));
                        }
                    }
                }
            }

            JoinType::Left => {
                for left_row in &left_rows {
                    let mut matched = false;

                    for right_row in &right_rows {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .chain(right_row.values.iter())
                            .cloned()
                            .collect();

                        let matches = if let Some(cond) = condition {
                            let combined_row = Row::new(combined_values.clone());
                            match evaluate_expression(
                                cond,
                                &combined_row,
                                &combined_schema,
                                &self.functions,
                            )? {
                                Value::Boolean(b) => b,
                                _ => false,
                            }
                        } else {
                            true
                        };

                        if matches {
                            result.push(Row::new(combined_values));
                            matched = true;
                        }
                    }

                    if !matched {
                        // Add left row with nulls for right side
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .cloned()
                            .chain(std::iter::repeat(Value::Null).take(right_schema.columns.len()))
                            .collect();
                        result.push(Row::new(combined_values));
                    }
                }
            }

            JoinType::Right => {
                for right_row in &right_rows {
                    let mut matched = false;

                    for left_row in &left_rows {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .chain(right_row.values.iter())
                            .cloned()
                            .collect();

                        let matches = if let Some(cond) = condition {
                            let combined_row = Row::new(combined_values.clone());
                            match evaluate_expression(
                                cond,
                                &combined_row,
                                &combined_schema,
                                &self.functions,
                            )? {
                                Value::Boolean(b) => b,
                                _ => false,
                            }
                        } else {
                            true
                        };

                        if matches {
                            result.push(Row::new(combined_values));
                            matched = true;
                        }
                    }

                    if !matched {
                        // Add nulls for left side with right row
                        let combined_values: Vec<Value> = std::iter::repeat(Value::Null)
                            .take(left_schema.columns.len())
                            .chain(right_row.values.iter().cloned())
                            .collect();
                        result.push(Row::new(combined_values));
                    }
                }
            }

            JoinType::Full => {
                let mut right_matched = vec![false; right_rows.len()];

                for left_row in &left_rows {
                    let mut left_matched = false;

                    for (i, right_row) in right_rows.iter().enumerate() {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .chain(right_row.values.iter())
                            .cloned()
                            .collect();

                        let matches = if let Some(cond) = condition {
                            let combined_row = Row::new(combined_values.clone());
                            match evaluate_expression(
                                cond,
                                &combined_row,
                                &combined_schema,
                                &self.functions,
                            )? {
                                Value::Boolean(b) => b,
                                _ => false,
                            }
                        } else {
                            true
                        };

                        if matches {
                            result.push(Row::new(combined_values));
                            left_matched = true;
                            right_matched[i] = true;
                        }
                    }

                    if !left_matched {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .cloned()
                            .chain(std::iter::repeat(Value::Null).take(right_schema.columns.len()))
                            .collect();
                        result.push(Row::new(combined_values));
                    }
                }

                // Add unmatched right rows
                for (i, right_row) in right_rows.iter().enumerate() {
                    if !right_matched[i] {
                        let combined_values: Vec<Value> = std::iter::repeat(Value::Null)
                            .take(left_schema.columns.len())
                            .chain(right_row.values.iter().cloned())
                            .collect();
                        result.push(Row::new(combined_values));
                    }
                }
            }

            JoinType::Cross => {
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let combined_values: Vec<Value> = left_row
                            .values
                            .iter()
                            .chain(right_row.values.iter())
                            .cloned()
                            .collect();
                        result.push(Row::new(combined_values));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute a streaming query
    pub async fn execute_streaming(
        &self,
        plan: PhysicalPlan,
    ) -> Result<mpsc::Receiver<Result<StreamResult>>> {
        let (tx, rx) = mpsc::channel(16);

        let result = self.execute(plan).await?;

        // Send the result
        let stream_result =
            StreamResult::new(result.schema.clone(), mpsc::channel::<Result<Row>>(16).1);

        let _ = tx.send(Ok(stream_result)).await;

        Ok(rx)
    }
}

/// Convert AST window type to window module type
fn convert_ast_window_type(ast_window: &super::ast::WindowType) -> Result<WindowType> {
    match ast_window {
        super::ast::WindowType::Tumbling { duration } => Ok(WindowType::Tumbling {
            duration_ms: duration.as_millis(),
        }),
        super::ast::WindowType::Hopping { duration, hop } => Ok(WindowType::Hopping {
            duration_ms: duration.as_millis(),
            hop_ms: hop.as_millis(),
        }),
        super::ast::WindowType::Sliding { duration } => Ok(WindowType::Sliding {
            duration_ms: duration.as_millis(),
        }),
        super::ast::WindowType::Session { gap } => Ok(WindowType::Session {
            gap_ms: gap.as_millis(),
        }),
        super::ast::WindowType::Count { size } => Ok(WindowType::Count { size: *size }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int64),
            Column::new("name", DataType::String),
        ]);

        let rows = vec![
            Row::new(vec![Value::Int64(1), Value::String("alice".into())]),
            Row::new(vec![Value::Int64(2), Value::String("bob".into())]),
        ];

        let result = QueryResult::new(schema, rows);
        assert_eq!(result.row_count(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_empty_query_result() {
        let schema = Schema::new(vec![Column::new("id", DataType::Int64)]);
        let result = QueryResult::empty(schema);
        assert_eq!(result.row_count(), 0);
        assert!(result.is_empty());
    }
}
