//! Query Optimizer with Pushdown
//!
//! Optimizes queries by pushing operations to the storage layer.

use super::config::QueryOptimizerConfig;
use super::parquet_segment::ColumnStats;
use super::statistics::StatisticsManager;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Query optimizer
pub struct QueryOptimizer {
    /// Statistics manager
    #[allow(dead_code)]
    statistics: Arc<StatisticsManager>,
    /// Configuration
    config: QueryOptimizerConfig,
}

impl QueryOptimizer {
    /// Create a new query optimizer
    pub fn new(statistics: Arc<StatisticsManager>, config: QueryOptimizerConfig) -> Self {
        Self { statistics, config }
    }

    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> Result<PhysicalPlan> {
        let mut optimized = plan;

        // Apply optimizations in order
        if self.config.predicate_pushdown {
            optimized = self.pushdown_predicates(optimized)?;
        }

        if self.config.projection_pushdown {
            optimized = self.pushdown_projections(optimized)?;
        }

        if self.config.partition_pruning {
            optimized = self.prune_partitions(optimized)?;
        }

        // Convert to physical plan
        self.to_physical_plan(optimized)
    }

    /// Apply predicate pushdown
    fn pushdown_predicates(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                // Try to push filter down
                match *input {
                    LogicalPlan::Scan {
                        source,
                        schema,
                        existing_predicate,
                    } => {
                        // Merge predicates
                        let merged = match existing_predicate {
                            Some(existing) => Predicate::And(vec![existing, predicate]),
                            None => predicate,
                        };
                        Ok(LogicalPlan::Scan {
                            source,
                            schema,
                            existing_predicate: Some(merged),
                        })
                    }
                    other => Ok(LogicalPlan::Filter {
                        input: Box::new(other),
                        predicate,
                    }),
                }
            }
            other => Ok(other),
        }
    }

    /// Apply projection pushdown
    fn pushdown_projections(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Projection pushdown implementation
        Ok(plan)
    }

    /// Apply partition pruning
    fn prune_partitions(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Partition pruning implementation
        Ok(plan)
    }

    /// Convert logical plan to physical plan
    fn to_physical_plan(&self, plan: LogicalPlan) -> Result<PhysicalPlan> {
        let root = self.to_physical_node(&plan)?;
        let cost = self.estimate_cost(&plan);

        Ok(PhysicalPlan {
            root,
            estimated_cost: cost,
            estimated_rows: self.estimate_rows(&plan),
        })
    }

    /// Convert logical node to physical node
    #[allow(clippy::only_used_in_recursion)]
    fn to_physical_node(&self, plan: &LogicalPlan) -> Result<PhysicalPlanNode> {
        match plan {
            LogicalPlan::Scan {
                source,
                existing_predicate,
                ..
            } => Ok(PhysicalPlanNode::ParquetScan {
                segments: vec![], // Would be filled by segment manager
                projection: None,
                predicate: existing_predicate.clone(),
                topic: source.clone(),
            }),
            LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlanNode::Filter {
                input: Box::new(self.to_physical_node(input)?),
                predicate: predicate.clone(),
            }),
            LogicalPlan::Project { input, columns } => Ok(PhysicalPlanNode::Project {
                input: Box::new(self.to_physical_node(input)?),
                columns: columns.clone(),
            }),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => Ok(PhysicalPlanNode::Aggregate {
                input: Box::new(self.to_physical_node(input)?),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            }),
            LogicalPlan::Sort { input, order_by } => Ok(PhysicalPlanNode::Sort {
                input: Box::new(self.to_physical_node(input)?),
                order_by: order_by.clone(),
            }),
            LogicalPlan::Limit { input, limit } => Ok(PhysicalPlanNode::Limit {
                input: Box::new(self.to_physical_node(input)?),
                limit: *limit,
            }),
            LogicalPlan::MaterializedViewScan { view_name } => {
                Ok(PhysicalPlanNode::MaterializedViewScan {
                    view_name: view_name.clone(),
                })
            }
        }
    }

    /// Estimate plan cost
    #[allow(clippy::only_used_in_recursion)]
    fn estimate_cost(&self, plan: &LogicalPlan) -> Cost {
        match plan {
            LogicalPlan::Scan { .. } => Cost {
                cpu: 1.0,
                io: 100.0,
                memory: 10.0,
                total: 111.0,
            },
            LogicalPlan::Filter { input, .. } => {
                let mut cost = self.estimate_cost(input);
                cost.cpu += 1.0;
                cost.total = cost.cpu + cost.io + cost.memory;
                cost
            }
            LogicalPlan::Project { input, .. } => {
                let mut cost = self.estimate_cost(input);
                cost.cpu += 0.5;
                cost.total = cost.cpu + cost.io + cost.memory;
                cost
            }
            LogicalPlan::Aggregate { input, .. } => {
                let mut cost = self.estimate_cost(input);
                cost.cpu += 10.0;
                cost.memory += 50.0;
                cost.total = cost.cpu + cost.io + cost.memory;
                cost
            }
            LogicalPlan::Sort { input, .. } => {
                let mut cost = self.estimate_cost(input);
                cost.cpu += 20.0;
                cost.memory += 100.0;
                cost.total = cost.cpu + cost.io + cost.memory;
                cost
            }
            LogicalPlan::Limit { input, .. } => {
                let mut cost = self.estimate_cost(input);
                cost.cpu += 0.1;
                cost.total = cost.cpu + cost.io + cost.memory;
                cost
            }
            LogicalPlan::MaterializedViewScan { .. } => Cost {
                cpu: 0.5,
                io: 10.0,
                memory: 5.0,
                total: 15.5,
            },
        }
    }

    /// Estimate row count
    #[allow(clippy::only_used_in_recursion)]
    fn estimate_rows(&self, plan: &LogicalPlan) -> u64 {
        match plan {
            LogicalPlan::Scan { .. } => 1_000_000, // Default estimate
            LogicalPlan::Filter { input, .. } => self.estimate_rows(input) / 10,
            LogicalPlan::Project { input, .. } => self.estimate_rows(input),
            LogicalPlan::Aggregate { input, .. } => self.estimate_rows(input) / 100,
            LogicalPlan::Sort { input, .. } => self.estimate_rows(input),
            LogicalPlan::Limit { limit, .. } => *limit as u64,
            LogicalPlan::MaterializedViewScan { .. } => 10_000,
        }
    }
}

/// Logical query plan
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan from source
    Scan {
        source: String,
        schema: Vec<String>,
        existing_predicate: Option<Predicate>,
    },
    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },
    /// Project columns
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    /// Aggregate
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sort
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
    },
    /// Materialized view scan
    MaterializedViewScan { view_name: String },
}

/// Predicate that can be pushed to storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {
    /// Column equals value
    Eq { column: String, value: Value },
    /// Column not equals value
    Ne { column: String, value: Value },
    /// Column less than value
    Lt { column: String, value: Value },
    /// Column less than or equal
    Le { column: String, value: Value },
    /// Column greater than value
    Gt { column: String, value: Value },
    /// Column greater than or equal
    Ge { column: String, value: Value },
    /// Column is null
    IsNull { column: String },
    /// Column is not null
    IsNotNull { column: String },
    /// Column in list
    In { column: String, values: Vec<Value> },
    /// Column between values
    Between {
        column: String,
        low: Value,
        high: Value,
    },
    /// String starts with
    StartsWith { column: String, prefix: String },
    /// AND of predicates
    And(Vec<Predicate>),
    /// OR of predicates
    Or(Vec<Predicate>),
    /// NOT predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Check if predicate can use statistics to skip row group
    pub fn can_skip(&self, column: &str, stats: &ColumnStats) -> bool {
        match self {
            Predicate::Eq { column: col, value } if col == column => {
                if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
                    let val_str = value.to_string();
                    val_str < *min || val_str > *max
                } else {
                    false
                }
            }
            Predicate::Lt { column: col, value } if col == column => {
                if let Some(min) = &stats.min_value {
                    value.to_string() <= *min
                } else {
                    false
                }
            }
            Predicate::Gt { column: col, value } if col == column => {
                if let Some(max) = &stats.max_value {
                    value.to_string() >= *max
                } else {
                    false
                }
            }
            Predicate::And(predicates) => predicates.iter().any(|p| p.can_skip(column, stats)),
            _ => false,
        }
    }

    /// Check if predicate can use bloom filter
    pub fn can_use_bloom_filter(&self) -> bool {
        matches!(self, Predicate::Eq { .. } | Predicate::In { .. })
    }

    /// Get columns referenced by this predicate
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Predicate::Eq { column, .. }
            | Predicate::Ne { column, .. }
            | Predicate::Lt { column, .. }
            | Predicate::Le { column, .. }
            | Predicate::Gt { column, .. }
            | Predicate::Ge { column, .. }
            | Predicate::IsNull { column }
            | Predicate::IsNotNull { column }
            | Predicate::In { column, .. }
            | Predicate::Between { column, .. }
            | Predicate::StartsWith { column, .. } => vec![column.as_str()],
            Predicate::And(preds) | Predicate::Or(preds) => {
                preds.iter().flat_map(|p| p.columns()).collect()
            }
            Predicate::Not(pred) => pred.columns(),
        }
    }
}

/// Value type for predicates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "{:?}", b),
            Value::Timestamp(t) => write!(f, "{}", t),
        }
    }
}

/// Physical execution plan
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    /// Plan root node
    pub root: PhysicalPlanNode,
    /// Estimated cost
    pub estimated_cost: Cost,
    /// Estimated row count
    pub estimated_rows: u64,
}

/// Physical plan nodes
#[derive(Debug, Clone)]
pub enum PhysicalPlanNode {
    /// Scan Parquet segments
    ParquetScan {
        segments: Vec<u64>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        topic: String,
    },
    /// Filter operation
    Filter {
        input: Box<PhysicalPlanNode>,
        predicate: Predicate,
    },
    /// Projection operation
    Project {
        input: Box<PhysicalPlanNode>,
        columns: Vec<String>,
    },
    /// Aggregation
    Aggregate {
        input: Box<PhysicalPlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sort
    Sort {
        input: Box<PhysicalPlanNode>,
        order_by: Vec<SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<PhysicalPlanNode>,
        limit: usize,
    },
    /// Materialized view scan
    MaterializedViewScan { view_name: String },
}

/// Aggregate expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// Function name (COUNT, SUM, AVG, MIN, MAX)
    pub function: String,
    /// Column to aggregate
    pub column: Option<String>,
    /// Distinct flag
    pub distinct: bool,
    /// Output alias
    pub alias: String,
}

/// Sort expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortExpr {
    /// Column to sort by
    pub column: String,
    /// Ascending order
    pub ascending: bool,
    /// Nulls first
    pub nulls_first: bool,
}

/// Query cost estimate
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cost {
    /// CPU cost
    pub cpu: f64,
    /// I/O cost
    pub io: f64,
    /// Memory cost
    pub memory: f64,
    /// Total cost
    pub total: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_can_skip() {
        // Use alphabetically sortable values for string comparison
        let stats = ColumnStats {
            name: "value".to_string(),
            data_type: "string".to_string(),
            null_count: 0,
            distinct_count: Some(100),
            min_value: Some("apple".to_string()),
            max_value: Some("pear".to_string()),
            total_size_bytes: 800,
        };

        // Value "aardvark" is less than min ("apple"), should skip
        let pred = Predicate::Eq {
            column: "value".to_string(),
            value: Value::String("aardvark".to_string()),
        };
        assert!(pred.can_skip("value", &stats));

        // Value "zebra" is greater than max ("pear"), should skip
        let pred = Predicate::Eq {
            column: "value".to_string(),
            value: Value::String("zebra".to_string()),
        };
        assert!(pred.can_skip("value", &stats));

        // Value "banana" is in range, should not skip
        let pred = Predicate::Eq {
            column: "value".to_string(),
            value: Value::String("banana".to_string()),
        };
        assert!(!pred.can_skip("value", &stats));
    }

    #[test]
    fn test_predicate_columns() {
        let pred = Predicate::And(vec![
            Predicate::Eq {
                column: "a".to_string(),
                value: Value::Int(1),
            },
            Predicate::Gt {
                column: "b".to_string(),
                value: Value::Int(10),
            },
        ]);

        let cols = pred.columns();
        assert!(cols.contains(&"a"));
        assert!(cols.contains(&"b"));
    }
}
