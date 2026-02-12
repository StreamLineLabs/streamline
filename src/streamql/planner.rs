//! Query planner for StreamlineQL
//!
//! Transforms AST into logical and physical execution plans.

use super::ast::*;
use super::functions::FunctionRegistry;
use super::types::{Column, DataType, Schema};
use crate::error::{Result, StreamlineError};

/// Logical plan node
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a stream/table
    Scan {
        source: String,
        schema: Schema,
        alias: Option<String>,
    },
    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression,
    },
    /// Project columns
    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<(Expression, String)>,
        schema: Schema,
    },
    /// Aggregate rows
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expression>,
        aggregates: Vec<(String, Expression, bool, String)>, // (func, arg, distinct, alias)
        schema: Schema,
    },
    /// Window aggregate
    Window {
        input: Box<LogicalPlan>,
        window_type: WindowType,
        partition_by: Option<Vec<Expression>>,
        group_by: Vec<Expression>,
        aggregates: Vec<(String, Expression, bool, String)>,
        schema: Schema,
    },
    /// Sort rows
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<(Expression, bool, bool)>, // (expr, ascending, nulls_first)
    },
    /// Limit rows
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },
    /// Distinct rows
    Distinct { input: Box<LogicalPlan> },
    /// Join two plans
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expression>,
        schema: Schema,
    },
    /// Union two plans
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },
}

impl LogicalPlan {
    /// Get the output schema of this plan
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Window { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Distinct { input } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::Union { left, .. } => left.schema(),
        }
    }

    /// Get a human-readable explanation of the plan
    pub fn explain(&self) -> String {
        self.explain_indent(0)
    }

    fn explain_indent(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        match self {
            LogicalPlan::Scan { source, alias, .. } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                format!("{}Scan: {}{}\n", prefix, source, alias_str)
            }
            LogicalPlan::Filter { input, predicate } => {
                format!(
                    "{}Filter: {:?}\n{}",
                    prefix,
                    predicate,
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Project {
                input, expressions, ..
            } => {
                let exprs: Vec<String> =
                    expressions.iter().map(|(_, alias)| alias.clone()).collect();
                format!(
                    "{}Project: [{}]\n{}",
                    prefix,
                    exprs.join(", "),
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                ..
            } => {
                let groups: Vec<String> = group_by.iter().map(|e| format!("{:?}", e)).collect();
                let aggs: Vec<String> = aggregates
                    .iter()
                    .map(|(f, _, _, a)| format!("{}({})", f, a))
                    .collect();
                format!(
                    "{}Aggregate: group=[{}], aggs=[{}]\n{}",
                    prefix,
                    groups.join(", "),
                    aggs.join(", "),
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Window {
                input, window_type, ..
            } => {
                format!(
                    "{}Window: {:?}\n{}",
                    prefix,
                    window_type,
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Sort { input, order_by } => {
                let sorts: Vec<String> = order_by
                    .iter()
                    .map(|(_, asc, _)| if *asc { "ASC" } else { "DESC" }.to_string())
                    .collect();
                format!(
                    "{}Sort: [{}]\n{}",
                    prefix,
                    sorts.join(", "),
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                format!(
                    "{}Limit: {} offset {}\n{}",
                    prefix,
                    limit,
                    offset,
                    input.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Distinct { input } => {
                format!("{}Distinct\n{}", prefix, input.explain_indent(indent + 1))
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                format!(
                    "{}Join: {:?}\n{}{}",
                    prefix,
                    join_type,
                    left.explain_indent(indent + 1),
                    right.explain_indent(indent + 1)
                )
            }
            LogicalPlan::Union { left, right, all } => {
                format!(
                    "{}Union{}\n{}{}",
                    prefix,
                    if *all { " All" } else { "" },
                    left.explain_indent(indent + 1),
                    right.explain_indent(indent + 1)
                )
            }
        }
    }
}

/// Physical execution plan
#[derive(Debug)]
pub struct PhysicalPlan {
    /// Logical plan to execute
    pub logical: LogicalPlan,
}

impl PhysicalPlan {
    /// Get execution explanation
    pub fn explain(&self) -> String {
        self.logical.explain()
    }
}

/// Query planner
pub struct QueryPlanner {
    /// Function registry for resolving functions
    functions: FunctionRegistry,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            functions: FunctionRegistry::new(),
        }
    }

    /// Plan a statement
    pub fn plan(&self, stmt: &Statement) -> Result<PhysicalPlan> {
        let logical = match stmt {
            Statement::Select(select) => self.plan_select(select.as_ref())?,
            Statement::Explain(inner) => {
                let plan = self.plan(inner)?;
                return Ok(plan);
            }
            _ => {
                return Err(StreamlineError::Query(
                    "Only SELECT statements are currently supported".to_string(),
                ))
            }
        };

        Ok(PhysicalPlan { logical })
    }

    fn plan_select(&self, select: &SelectStatement) -> Result<LogicalPlan> {
        // Start with the source scan
        let mut plan = self.plan_from(&select.from)?;

        // Apply filter (WHERE)
        if let Some(ref filter) = select.filter {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: filter.clone(),
            };
        }

        // Check for aggregation or window
        let (has_agg, aggregates) = self.find_aggregates(&select.projections)?;

        if let Some(ref window_spec) = select.window {
            // Window aggregation
            let group_by = select.group_by.clone().unwrap_or_default();
            let output_schema = self.build_window_schema(&group_by, &aggregates, plan.schema())?;

            plan = LogicalPlan::Window {
                input: Box::new(plan),
                window_type: window_spec.window_type.clone(),
                partition_by: window_spec.partition_by.clone(),
                group_by,
                aggregates,
                schema: output_schema,
            };
        } else if has_agg || select.group_by.is_some() {
            // Regular aggregation
            let group_by = select.group_by.clone().unwrap_or_default();
            let output_schema =
                self.build_aggregate_schema(&group_by, &aggregates, plan.schema())?;

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
                schema: output_schema,
            };

            // Apply HAVING
            if let Some(ref having) = select.having {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: having.clone(),
                };
            }
        } else {
            // Simple projection
            let (expressions, output_schema) =
                self.build_projections(&select.projections, plan.schema())?;

            plan = LogicalPlan::Project {
                input: Box::new(plan),
                expressions,
                schema: output_schema,
            };
        }

        // Apply DISTINCT
        if select.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        // Apply ORDER BY
        if let Some(ref order_by) = select.order_by {
            let sort_specs: Vec<_> = order_by
                .iter()
                .map(|o| (o.expr.clone(), o.ascending, o.nulls_first))
                .collect();

            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: sort_specs,
            };
        }

        // Apply LIMIT/OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: select.limit.unwrap_or(usize::MAX),
                offset: select.offset.unwrap_or(0),
            };
        }

        Ok(plan)
    }

    fn plan_from(&self, from: &FromClause) -> Result<LogicalPlan> {
        let mut plan = self.plan_table_ref(&from.source)?;

        for join in &from.joins {
            let right = self.plan_table_ref(&join.right)?;
            let condition = match &join.condition {
                JoinCondition::On(expr) => Some(expr.clone()),
                JoinCondition::Using(cols) => {
                    // Build equality condition for USING
                    self.build_using_condition(cols, plan.schema(), right.schema())?
                }
                JoinCondition::Natural => {
                    // Find common columns
                    self.build_natural_condition(plan.schema(), right.schema())?
                }
                JoinCondition::None => None,
            };

            let schema = self.build_join_schema(plan.schema(), right.schema(), &join.join_type)?;

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: join.join_type,
                condition,
                schema,
            };
        }

        Ok(plan)
    }

    fn plan_table_ref(&self, table_ref: &TableRef) -> Result<LogicalPlan> {
        match table_ref {
            TableRef::Table { name, alias } => {
                // Default schema for a topic - will be inferred from data
                let schema = Schema::new(vec![
                    Column::new("offset", DataType::Int64),
                    Column::new("timestamp", DataType::Timestamp),
                    Column::new("key", DataType::Binary),
                    Column::new("value", DataType::Binary),
                    Column::new("partition", DataType::Int64),
                ]);

                Ok(LogicalPlan::Scan {
                    source: name.clone(),
                    schema,
                    alias: alias.clone(),
                })
            }
            TableRef::Function { name, args, alias } => {
                // Handle table-valued functions like streamline_topic()
                if name.to_lowercase() == "streamline_topic" {
                    if let Some(Expression::Literal(Literal::String(topic_name))) = args.first() {
                        let schema = Schema::new(vec![
                            Column::new("offset", DataType::Int64),
                            Column::new("timestamp", DataType::Timestamp),
                            Column::new("key", DataType::Binary),
                            Column::new("value", DataType::Binary),
                            Column::new("partition", DataType::Int64),
                        ]);

                        return Ok(LogicalPlan::Scan {
                            source: topic_name.clone(),
                            schema,
                            alias: alias.clone(),
                        });
                    }
                }

                Err(StreamlineError::Query(format!(
                    "Unknown table function: {}",
                    name
                )))
            }
            TableRef::Subquery { query, alias } => {
                let subplan = self.plan_select(query)?;
                // Wrap in a scan with the subquery alias
                Ok(LogicalPlan::Scan {
                    source: format!("(subquery:{})", alias),
                    schema: subplan.schema().clone(),
                    alias: Some(alias.clone()),
                })
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn find_aggregates(
        &self,
        projections: &[SelectItem],
    ) -> Result<(bool, Vec<(String, Expression, bool, String)>)> {
        let mut has_agg = false;
        let mut aggregates = Vec::new();

        for (i, item) in projections.iter().enumerate() {
            if let SelectItem::Expression { expr, alias } = item {
                self.collect_aggregates(expr, &mut aggregates, alias.as_deref(), i, &mut has_agg)?;
            }
        }

        Ok((has_agg, aggregates))
    }

    fn collect_aggregates(
        &self,
        expr: &Expression,
        aggregates: &mut Vec<(String, Expression, bool, String)>,
        alias: Option<&str>,
        index: usize,
        has_agg: &mut bool,
    ) -> Result<()> {
        match expr {
            Expression::Function {
                name,
                args,
                distinct,
            } => {
                if self.functions.is_aggregate(name) {
                    *has_agg = true;
                    let arg = args
                        .first()
                        .cloned()
                        .unwrap_or(Expression::Literal(Literal::Null));
                    let col_alias = alias
                        .map(String::from)
                        .unwrap_or_else(|| format!("agg_{}", index));
                    aggregates.push((name.clone(), arg, *distinct, col_alias));
                } else {
                    for arg in args {
                        self.collect_aggregates(arg, aggregates, None, index, has_agg)?;
                    }
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.collect_aggregates(left, aggregates, None, index, has_agg)?;
                self.collect_aggregates(right, aggregates, None, index, has_agg)?;
            }
            Expression::UnaryOp { expr, .. } => {
                self.collect_aggregates(expr, aggregates, None, index, has_agg)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn build_projections(
        &self,
        projections: &[SelectItem],
        input_schema: &Schema,
    ) -> Result<(Vec<(Expression, String)>, Schema)> {
        let mut expressions = Vec::new();
        let mut columns = Vec::new();

        for (i, item) in projections.iter().enumerate() {
            match item {
                SelectItem::Wildcard => {
                    for col in &input_schema.columns {
                        expressions.push((
                            Expression::Column(ColumnRef::simple(&col.name)),
                            col.name.clone(),
                        ));
                        columns.push(col.clone());
                    }
                }
                SelectItem::QualifiedWildcard(table) => {
                    return Err(StreamlineError::Query(format!(
                        "Qualified wildcard '{}' is not supported yet",
                        table
                    )));
                }
                SelectItem::Expression { expr, alias } => {
                    let col_name = alias.clone().unwrap_or_else(|| self.infer_alias(expr, i));
                    let data_type = self.infer_type(expr, input_schema)?;
                    expressions.push((expr.clone(), col_name.clone()));
                    columns.push(Column::new(col_name, data_type));
                }
            }
        }

        Ok((expressions, Schema::new(columns)))
    }

    fn build_aggregate_schema(
        &self,
        group_by: &[Expression],
        aggregates: &[(String, Expression, bool, String)],
        input_schema: &Schema,
    ) -> Result<Schema> {
        let mut columns = Vec::new();

        // Group by columns
        for (i, expr) in group_by.iter().enumerate() {
            let name = self.infer_alias(expr, i);
            let data_type = self.infer_type(expr, input_schema)?;
            columns.push(Column::new(name, data_type));
        }

        // Aggregate columns
        for (func_name, _, _, alias) in aggregates {
            let data_type = match func_name.to_uppercase().as_str() {
                "COUNT" => DataType::Int64,
                "SUM" | "AVG" => DataType::Float64,
                _ => DataType::Null,
            };
            columns.push(Column::new(alias.clone(), data_type));
        }

        Ok(Schema::new(columns))
    }

    fn build_window_schema(
        &self,
        group_by: &[Expression],
        aggregates: &[(String, Expression, bool, String)],
        input_schema: &Schema,
    ) -> Result<Schema> {
        let mut columns = vec![
            Column::new("window_start", DataType::Timestamp),
            Column::new("window_end", DataType::Timestamp),
        ];

        // Add group by and aggregate columns
        let agg_schema = self.build_aggregate_schema(group_by, aggregates, input_schema)?;
        columns.extend(agg_schema.columns);

        Ok(Schema::new(columns))
    }

    fn build_join_schema(
        &self,
        left: &Schema,
        right: &Schema,
        _join_type: &JoinType,
    ) -> Result<Schema> {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        Ok(Schema::new(columns))
    }

    fn build_using_condition(
        &self,
        cols: &[String],
        _left: &Schema,
        _right: &Schema,
    ) -> Result<Option<Expression>> {
        if cols.is_empty() {
            return Ok(None);
        }

        let mut conditions = Vec::new();
        for col in cols {
            conditions.push(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnRef::simple(col))),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column(ColumnRef::simple(col))),
            });
        }

        Ok(conditions
            .into_iter()
            .reduce(|a, b| Expression::BinaryOp {
                left: Box::new(a),
                op: BinaryOperator::And,
                right: Box::new(b),
            }))
    }

    fn build_natural_condition(&self, left: &Schema, right: &Schema) -> Result<Option<Expression>> {
        let mut common_cols = Vec::new();
        for l_col in &left.columns {
            for r_col in &right.columns {
                if l_col.name == r_col.name {
                    common_cols.push(l_col.name.clone());
                }
            }
        }

        self.build_using_condition(&common_cols, left, right)
    }

    fn infer_alias(&self, expr: &Expression, index: usize) -> String {
        match expr {
            Expression::Column(col_ref) => col_ref.column.clone(),
            Expression::Function { name, .. } => name.to_lowercase(),
            _ => format!("expr_{}", index),
        }
    }

    fn infer_type(&self, expr: &Expression, schema: &Schema) -> Result<DataType> {
        match expr {
            Expression::Literal(lit) => Ok(match lit {
                Literal::Null => DataType::Null,
                Literal::Boolean(_) => DataType::Boolean,
                Literal::Integer(_) => DataType::Int64,
                Literal::Float(_) => DataType::Float64,
                Literal::String(_) => DataType::String,
                Literal::Interval(_) => DataType::Duration,
            }),
            Expression::Column(col_ref) => {
                if let Some(idx) = schema.column_index(&col_ref.column) {
                    Ok(schema.columns[idx].data_type.clone())
                } else {
                    Ok(DataType::Null)
                }
            }
            Expression::Function { name, .. } => {
                if let Some(func) = self.functions.get_scalar(name) {
                    Ok(func.return_type.clone())
                } else if let Some(func) = self.functions.get_aggregate(name) {
                    Ok(func.return_type.clone())
                } else {
                    Ok(DataType::Null)
                }
            }
            Expression::BinaryOp { left, op, right } => {
                let left_type = self.infer_type(left, schema)?;
                let right_type = self.infer_type(right, schema)?;

                match op {
                    BinaryOperator::Equal
                    | BinaryOperator::NotEqual
                    | BinaryOperator::LessThan
                    | BinaryOperator::LessThanOrEqual
                    | BinaryOperator::GreaterThan
                    | BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::And
                    | BinaryOperator::Or => Ok(DataType::Boolean),
                    BinaryOperator::Concat => Ok(DataType::String),
                    _ => {
                        // Return the "wider" numeric type
                        if left_type == DataType::Float64 || right_type == DataType::Float64 {
                            Ok(DataType::Float64)
                        } else {
                            Ok(DataType::Int64)
                        }
                    }
                }
            }
            Expression::Cast { data_type, .. } => Ok(data_type.clone()),
            _ => Ok(DataType::Null),
        }
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::parser::StreamQLParser;
    use super::*;

    #[test]
    fn test_plan_simple_select() {
        let stmt = StreamQLParser::parse("SELECT * FROM events").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt).unwrap();

        let explain = plan.explain();
        assert!(explain.contains("Scan: events"));
    }

    #[test]
    fn test_plan_select_with_filter() {
        let stmt = StreamQLParser::parse("SELECT * FROM events WHERE value > 10").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt).unwrap();

        let explain = plan.explain();
        assert!(explain.contains("Filter:"));
    }

    #[test]
    fn test_plan_qualified_wildcard_not_supported() {
        let stmt = StreamQLParser::parse("SELECT events.* FROM events").unwrap();
        let planner = QueryPlanner::new();
        let err = planner.plan(&stmt).unwrap_err();
        assert!(err.to_string().contains("Qualified wildcard"));
    }

    #[test]
    fn test_plan_aggregate() {
        let stmt =
            StreamQLParser::parse("SELECT user_id, COUNT(*) FROM events GROUP BY user_id").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt).unwrap();

        let explain = plan.explain();
        assert!(explain.contains("Aggregate:"));
    }

    #[test]
    fn test_plan_with_limit() {
        let stmt = StreamQLParser::parse("SELECT * FROM events LIMIT 10 OFFSET 5").unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt).unwrap();

        let explain = plan.explain();
        assert!(explain.contains("Limit: 10 offset 5"));
    }
}
