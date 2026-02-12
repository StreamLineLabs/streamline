//! Abstract Syntax Tree for StreamlineQL
//!
//! Defines the parsed representation of StreamlineQL queries.

use super::types::DataType;
use serde::{Deserialize, Serialize};

/// A complete StreamlineQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// SELECT query
    Select(Box<SelectStatement>),
    /// INSERT statement (for sinks)
    Insert(InsertStatement),
    /// CREATE STREAM/TABLE
    CreateStream(CreateStreamStatement),
    /// EXPLAIN query
    Explain(Box<Statement>),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// SELECT clause (columns and expressions)
    pub projections: Vec<SelectItem>,
    /// FROM clause
    pub from: FromClause,
    /// WHERE clause (optional filter)
    pub filter: Option<Expression>,
    /// GROUP BY clause (optional)
    pub group_by: Option<Vec<Expression>>,
    /// HAVING clause (optional filter on groups)
    pub having: Option<Expression>,
    /// WINDOW clause (optional)
    pub window: Option<WindowSpec>,
    /// ORDER BY clause (optional)
    pub order_by: Option<Vec<OrderByItem>>,
    /// LIMIT clause (optional)
    pub limit: Option<usize>,
    /// OFFSET clause (optional)
    pub offset: Option<usize>,
    /// DISTINCT modifier
    pub distinct: bool,
}

/// A single item in the SELECT clause
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// All columns (*)
    Wildcard,
    /// Qualified wildcard (table.*)
    QualifiedWildcard(String),
    /// Named expression with optional alias
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    /// Primary source
    pub source: TableRef,
    /// Joins
    pub joins: Vec<Join>,
}

/// Table reference
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Simple table/stream name
    Table { name: String, alias: Option<String> },
    /// Subquery
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
    /// Stream function (e.g., streamline_topic('name'))
    Function {
        name: String,
        args: Vec<Expression>,
        alias: Option<String>,
    },
}

/// Join clause
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// Type of join
    pub join_type: JoinType,
    /// Right side of the join
    pub right: TableRef,
    /// Join condition
    pub condition: JoinCondition,
}

/// Type of join
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Join condition
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    /// ON expression
    On(Expression),
    /// USING columns
    Using(Vec<String>),
    /// Natural join
    Natural,
    /// No condition (cross join)
    None,
}

/// Window specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    /// Window type
    pub window_type: WindowType,
    /// Partition by clause (optional)
    pub partition_by: Option<Vec<Expression>>,
}

/// Window types for stream processing
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Tumbling window (non-overlapping fixed intervals)
    Tumbling { duration: Duration },
    /// Hopping window (overlapping fixed intervals)
    Hopping { duration: Duration, hop: Duration },
    /// Sliding window (triggered on each event)
    Sliding { duration: Duration },
    /// Session window (based on activity)
    Session { gap: Duration },
    /// Count-based window
    Count { size: usize },
}

/// Duration specification
#[derive(Debug, Clone, PartialEq)]
pub struct Duration {
    /// Value
    pub value: u64,
    /// Unit
    pub unit: TimeUnit,
}

impl Duration {
    /// Create a new duration
    pub fn new(value: u64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    /// Convert to milliseconds
    pub fn as_millis(&self) -> u64 {
        match self.unit {
            TimeUnit::Millisecond => self.value,
            TimeUnit::Second => self.value * 1000,
            TimeUnit::Minute => self.value * 60 * 1000,
            TimeUnit::Hour => self.value * 60 * 60 * 1000,
            TimeUnit::Day => self.value * 24 * 60 * 60 * 1000,
        }
    }
}

/// Time unit for durations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    /// Expression to order by
    pub expr: Expression,
    /// Ascending (true) or descending (false)
    pub ascending: bool,
    /// Nulls first (true) or last (false)
    pub nulls_first: bool,
}

/// Expression in StreamlineQL
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Column reference
    Column(ColumnRef),
    /// Literal value
    Literal(Literal),
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
    Function {
        name: String,
        args: Vec<Expression>,
        distinct: bool,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    /// CAST expression
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
    /// IN expression
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    /// BETWEEN expression
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },
    /// LIKE expression
    Like {
        expr: Box<Expression>,
        pattern: String,
        escape: Option<char>,
        negated: bool,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool,
    },
    /// Nested expression (in parentheses)
    Nested(Box<Expression>),
    /// Subquery
    Subquery(Box<SelectStatement>),
    /// Aggregate over window
    WindowFunction {
        function: Box<Expression>,
        partition_by: Vec<Expression>,
        order_by: Vec<OrderByItem>,
        frame: Option<WindowFrame>,
    },
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    /// Frame type
    pub frame_type: WindowFrameType,
    /// Start bound
    pub start: WindowFrameBound,
    /// End bound (optional)
    pub end: Option<WindowFrameBound>,
}

/// Window frame type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameType {
    Rows,
    Range,
    Groups,
}

/// Window frame bound
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// N PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// N FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

/// Column reference
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    /// Table/stream alias (optional)
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

impl ColumnRef {
    /// Create a simple column reference
    pub fn simple(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Create a qualified column reference
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// NULL literal
    Null,
    /// Boolean literal
    Boolean(bool),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// String literal
    String(String),
    /// Interval literal
    Interval(Duration),
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Logical
    And,
    Or,
    // String
    Concat,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Negation (-)
    Minus,
    /// Logical NOT
    Not,
    /// Bitwise NOT
    BitwiseNot,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Target table/stream
    pub target: String,
    /// Columns (optional)
    pub columns: Option<Vec<String>>,
    /// Source (SELECT or VALUES)
    pub source: InsertSource,
}

/// Source for INSERT
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// SELECT query
    Query(Box<SelectStatement>),
    /// VALUES clause
    Values(Vec<Vec<Expression>>),
}

/// CREATE STREAM statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateStreamStatement {
    /// Stream name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// WITH options
    pub options: Vec<(String, String)>,
    /// If not exists
    pub if_not_exists: bool,
}

/// Column definition for CREATE
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// NOT NULL constraint
    pub not_null: bool,
    /// DEFAULT value
    pub default: Option<Expression>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_conversion() {
        let d = Duration::new(5, TimeUnit::Minute);
        assert_eq!(d.as_millis(), 300_000);

        let d = Duration::new(1, TimeUnit::Hour);
        assert_eq!(d.as_millis(), 3_600_000);
    }

    #[test]
    fn test_column_ref() {
        let col = ColumnRef::simple("id");
        assert_eq!(col.column, "id");
        assert_eq!(col.table, None);

        let col = ColumnRef::qualified("events", "user_id");
        assert_eq!(col.column, "user_id");
        assert_eq!(col.table, Some("events".to_string()));
    }
}
