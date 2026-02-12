//! Parser for StreamlineQL.
//!
//! Transforms tokens into an Abstract Syntax Tree (AST).
//!
//! # Supported Statements
//!
//! - `SELECT` queries with all standard SQL clauses
//! - `INSERT INTO ... VALUES` and `INSERT INTO ... SELECT`
//! - `CREATE STREAM/TABLE` for schema definition
//! - `EXPLAIN` for query plan inspection
//!
//! # Query Features
//!
//! The parser supports:
//! - Projections with aliases: `SELECT col AS alias`
//! - Wildcards: `SELECT *`, `SELECT table.*`
//! - Expressions: arithmetic, comparison, logical
//! - Functions: `COUNT(*)`, `SUM(value)`, `AVG(price)`
//! - Window functions: `SUM(value) OVER (PARTITION BY user_id)`
//! - CASE expressions: `CASE WHEN ... THEN ... ELSE ... END`
//! - Type casts: `CAST(value AS INTEGER)`
//! - Predicates: `IN`, `BETWEEN`, `LIKE`, `IS NULL`
//!
//! # Stream Processing Extensions
//!
//! - Window specifications: `WINDOW TUMBLING('1 minute')`
//! - Interval literals: `INTERVAL '5 minutes'`
//! - Time-based grouping: `GROUP BY window_start`
//!
//! # Example
//!
//! ```ignore
//! use streamline::streamql::parser::StreamQLParser;
//! use streamline::streamql::ast::Statement;
//!
//! let stmt = StreamQLParser::parse("SELECT * FROM events WHERE id > 10").unwrap();
//! if let Statement::Select(select) = stmt {
//!     assert!(select.filter.is_some());
//! }
//! ```

use super::ast::*;
use super::lexer::{Lexer, Token};
use super::types::DataType;
use crate::error::{Result, StreamlineError};

/// StreamlineQL parser
pub struct StreamQLParser {
    /// Tokens to parse
    tokens: Vec<Token>,
    /// Current position in tokens
    pos: usize,
}

impl StreamQLParser {
    /// Parse a query string into an AST
    pub fn parse(query: &str) -> Result<Statement> {
        let mut lexer = Lexer::new(query);
        let tokens = lexer.tokenize()?;
        let mut parser = Self { tokens, pos: 0 };
        parser.parse_statement()
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        if self.current() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(StreamlineError::Parse(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current()
            )))
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            Token::QuotedIdentifier(name) => {
                self.advance();
                Ok(name)
            }
            other => Err(StreamlineError::Parse(format!(
                "Expected identifier, found {:?}",
                other
            ))),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.current() {
            Token::Select => self.parse_select().map(|s| Statement::Select(Box::new(s))),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Create => self.parse_create(),
            Token::Explain => {
                self.advance();
                let stmt = self.parse_statement()?;
                Ok(Statement::Explain(Box::new(stmt)))
            }
            other => Err(StreamlineError::Parse(format!(
                "Unexpected token: {:?}",
                other
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(Token::Select)?;

        // Parse DISTINCT
        let distinct = if self.current() == &Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        // Parse projections
        let projections = self.parse_select_items()?;

        // Parse FROM
        self.expect(Token::From)?;
        let from = self.parse_from_clause()?;

        // Parse WHERE
        let filter = if self.current() == &Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse WINDOW
        let window = if self.current() == &Token::Window {
            self.advance();
            Some(self.parse_window_spec()?)
        } else {
            None
        };

        // Parse GROUP BY
        let group_by = if self.current() == &Token::Group {
            self.advance();
            self.expect(Token::By)?;
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        // Parse HAVING
        let having = if self.current() == &Token::Having {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.current() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            Some(self.parse_order_by_items()?)
        } else {
            None
        };

        // Parse LIMIT
        let limit = if self.current() == &Token::Limit {
            self.advance();
            match self.current().clone() {
                Token::IntegerLiteral(n) => {
                    self.advance();
                    Some(n as usize)
                }
                _ => {
                    return Err(StreamlineError::Parse(
                        "Expected integer after LIMIT".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        // Parse OFFSET
        let offset = if self.current() == &Token::Offset {
            self.advance();
            match self.current().clone() {
                Token::IntegerLiteral(n) => {
                    self.advance();
                    Some(n as usize)
                }
                _ => {
                    return Err(StreamlineError::Parse(
                        "Expected integer after OFFSET".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        Ok(SelectStatement {
            projections,
            from,
            filter,
            group_by,
            having,
            window,
            order_by,
            limit,
            offset,
            distinct,
        })
    }

    fn parse_select_items(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();

        loop {
            items.push(self.parse_select_item()?);

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        if self.current() == &Token::Asterisk {
            self.advance();
            return Ok(SelectItem::Wildcard);
        }

        // Check for qualified wildcard (table.*)
        if let Token::Identifier(name) = self.current().clone() {
            if self.peek() == &Token::Dot {
                self.advance(); // identifier
                self.advance(); // dot
                if self.current() == &Token::Asterisk {
                    self.advance();
                    return Ok(SelectItem::QualifiedWildcard(name));
                } else {
                    // Backtrack - it's actually a qualified column reference
                    self.pos -= 2;
                }
            }
        }

        let expr = self.parse_expression()?;

        let alias = if self.current() == &Token::As {
            self.advance();
            Some(self.expect_identifier()?)
        } else if let Token::Identifier(_) = self.current() {
            // Implicit alias
            Some(self.expect_identifier()?)
        } else {
            None
        };

        Ok(SelectItem::Expression { expr, alias })
    }

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let source = self.parse_table_ref()?;
        let mut joins = Vec::new();

        loop {
            let join_type = match self.current() {
                Token::Inner => {
                    self.advance();
                    self.expect(Token::Join)?;
                    JoinType::Inner
                }
                Token::Left => {
                    self.advance();
                    if self.current() == &Token::Outer {
                        self.advance();
                    }
                    self.expect(Token::Join)?;
                    JoinType::Left
                }
                Token::Right => {
                    self.advance();
                    if self.current() == &Token::Outer {
                        self.advance();
                    }
                    self.expect(Token::Join)?;
                    JoinType::Right
                }
                Token::Full => {
                    self.advance();
                    if self.current() == &Token::Outer {
                        self.advance();
                    }
                    self.expect(Token::Join)?;
                    JoinType::Full
                }
                Token::Cross => {
                    self.advance();
                    self.expect(Token::Join)?;
                    JoinType::Cross
                }
                Token::Join => {
                    self.advance();
                    JoinType::Inner
                }
                Token::Natural => {
                    self.advance();
                    let join_type = self.parse_join_type_modifier()?;
                    self.expect(Token::Join)?;
                    let right = self.parse_table_ref()?;
                    joins.push(Join {
                        join_type,
                        right,
                        condition: JoinCondition::Natural,
                    });
                    continue;
                }
                _ => break,
            };

            let right = self.parse_table_ref()?;

            let condition = if join_type == JoinType::Cross {
                JoinCondition::None
            } else if self.current() == &Token::On {
                self.advance();
                JoinCondition::On(self.parse_expression()?)
            } else if self.current() == &Token::Using {
                self.advance();
                self.expect(Token::LeftParen)?;
                let cols = self.parse_identifier_list()?;
                self.expect(Token::RightParen)?;
                JoinCondition::Using(cols)
            } else {
                JoinCondition::None
            };

            joins.push(Join {
                join_type,
                right,
                condition,
            });
        }

        Ok(FromClause { source, joins })
    }

    fn parse_join_type_modifier(&mut self) -> Result<JoinType> {
        match self.current() {
            Token::Left => {
                self.advance();
                if self.current() == &Token::Outer {
                    self.advance();
                }
                Ok(JoinType::Left)
            }
            Token::Right => {
                self.advance();
                if self.current() == &Token::Outer {
                    self.advance();
                }
                Ok(JoinType::Right)
            }
            Token::Full => {
                self.advance();
                if self.current() == &Token::Outer {
                    self.advance();
                }
                Ok(JoinType::Full)
            }
            Token::Inner => {
                self.advance();
                Ok(JoinType::Inner)
            }
            _ => Ok(JoinType::Inner),
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        if self.current() == &Token::LeftParen {
            // Subquery
            self.advance();
            let query = self.parse_select()?;
            self.expect(Token::RightParen)?;

            let alias = if self.current() == &Token::As {
                self.advance();
                self.expect_identifier()?
            } else {
                self.expect_identifier()?
            };

            Ok(TableRef::Subquery {
                query: Box::new(query),
                alias,
            })
        } else {
            let name = self.expect_identifier()?;

            // Check if it's a function call
            if self.current() == &Token::LeftParen {
                self.advance();
                let args = self.parse_expression_list()?;
                self.expect(Token::RightParen)?;

                let alias = if self.current() == &Token::As {
                    self.advance();
                    Some(self.expect_identifier()?)
                } else if let Token::Identifier(_) = self.current() {
                    Some(self.expect_identifier()?)
                } else {
                    None
                };

                Ok(TableRef::Function { name, args, alias })
            } else {
                let alias = if self.current() == &Token::As {
                    self.advance();
                    Some(self.expect_identifier()?)
                } else if let Token::Identifier(_) = self.current() {
                    // Check this isn't a keyword
                    if !self.current().is_keyword() {
                        Some(self.expect_identifier()?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(TableRef::Table { name, alias })
            }
        }
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec> {
        let window_type = self.parse_window_type()?;

        let partition_by = if self.current() == &Token::Partition {
            self.advance();
            self.expect(Token::By)?;
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        Ok(WindowSpec {
            window_type,
            partition_by,
        })
    }

    fn parse_window_type(&mut self) -> Result<WindowType> {
        match self.current() {
            Token::Tumbling => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let duration = self.parse_interval()?;
                self.expect(Token::RightParen)?;
                Ok(WindowType::Tumbling { duration })
            }
            Token::Hopping => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let duration = self.parse_interval()?;
                self.expect(Token::Comma)?;
                let hop = self.parse_interval()?;
                self.expect(Token::RightParen)?;
                Ok(WindowType::Hopping { duration, hop })
            }
            Token::Sliding => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let duration = self.parse_interval()?;
                self.expect(Token::RightParen)?;
                Ok(WindowType::Sliding { duration })
            }
            Token::Session => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let gap = self.parse_interval()?;
                self.expect(Token::RightParen)?;
                Ok(WindowType::Session { gap })
            }
            _ => Err(StreamlineError::Parse(
                "Expected window type (TUMBLING, HOPPING, SLIDING, SESSION)".to_string(),
            )),
        }
    }

    fn parse_interval(&mut self) -> Result<Duration> {
        if self.current() == &Token::Interval {
            self.advance();
        }

        // Parse the string literal or number + unit
        match self.current().clone() {
            Token::StringLiteral(s) => {
                self.advance();
                self.parse_interval_string(&s)
            }
            Token::IntegerLiteral(n) => {
                self.advance();
                let unit = self.parse_time_unit()?;
                Ok(Duration::new(n as u64, unit))
            }
            _ => Err(StreamlineError::Parse(
                "Expected interval value".to_string(),
            )),
        }
    }

    fn parse_interval_string(&self, s: &str) -> Result<Duration> {
        // Parse strings like "1 minute", "5 seconds", "30s", "1h"
        let s = s.trim().to_lowercase();

        // Try to find where the number ends
        let mut num_end = 0;
        for (i, c) in s.char_indices() {
            if c.is_ascii_digit() || c == '.' {
                num_end = i + c.len_utf8();
            } else if !c.is_whitespace() {
                break;
            }
        }

        let num_str = s[..num_end].trim();
        let unit_str = s[num_end..].trim();

        let value: u64 = num_str
            .parse()
            .map_err(|_| StreamlineError::Parse(format!("Invalid interval number: {}", num_str)))?;

        let unit = match unit_str {
            "ms" | "millisecond" | "milliseconds" => TimeUnit::Millisecond,
            "s" | "sec" | "second" | "seconds" => TimeUnit::Second,
            "m" | "min" | "minute" | "minutes" => TimeUnit::Minute,
            "h" | "hour" | "hours" => TimeUnit::Hour,
            "d" | "day" | "days" => TimeUnit::Day,
            "" => TimeUnit::Second, // Default to seconds
            _ => {
                return Err(StreamlineError::Parse(format!(
                    "Unknown time unit: {}",
                    unit_str
                )))
            }
        };

        Ok(Duration::new(value, unit))
    }

    fn parse_time_unit(&mut self) -> Result<TimeUnit> {
        match self.current() {
            Token::Identifier(s) => {
                let unit = match s.to_lowercase().as_str() {
                    "ms" | "millisecond" | "milliseconds" => TimeUnit::Millisecond,
                    "s" | "sec" | "second" | "seconds" => TimeUnit::Second,
                    "m" | "min" | "minute" | "minutes" => TimeUnit::Minute,
                    "h" | "hour" | "hours" => TimeUnit::Hour,
                    "d" | "day" | "days" => TimeUnit::Day,
                    _ => return Err(StreamlineError::Parse(format!("Unknown time unit: {}", s))),
                };
                self.advance();
                Ok(unit)
            }
            _ => Err(StreamlineError::Parse("Expected time unit".to_string())),
        }
    }

    fn parse_order_by_items(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            let ascending = match self.current() {
                Token::Asc => {
                    self.advance();
                    true
                }
                Token::Desc => {
                    self.advance();
                    false
                }
                _ => true, // Default to ascending
            };

            let nulls_first = if self.current() == &Token::Nulls {
                self.advance();
                match self.current() {
                    Token::First => {
                        self.advance();
                        true
                    }
                    Token::Last => {
                        self.advance();
                        false
                    }
                    _ => !ascending, // Default: NULLS FIRST for DESC, NULLS LAST for ASC
                }
            } else {
                !ascending
            };

            items.push(OrderByItem {
                expr,
                ascending,
                nulls_first,
            });

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_and_expression()?;

        while self.current() == &Token::Or {
            self.advance();
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_not_expression()?;

        while self.current() == &Token::And {
            self.advance();
            let right = self.parse_not_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expression(&mut self) -> Result<Expression> {
        if self.current() == &Token::Not {
            self.advance();
            let expr = self.parse_not_expression()?;
            Ok(Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expression()
        }
    }

    fn parse_comparison_expression(&mut self) -> Result<Expression> {
        let left = self.parse_additive_expression()?;

        match self.current() {
            Token::Equal => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Equal,
                    right: Box::new(right),
                })
            }
            Token::NotEqual => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::NotEqual,
                    right: Box::new(right),
                })
            }
            Token::LessThan => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::LessThan,
                    right: Box::new(right),
                })
            }
            Token::LessThanEq => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::LessThanOrEqual,
                    right: Box::new(right),
                })
            }
            Token::GreaterThan => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(right),
                })
            }
            Token::GreaterThanEq => {
                self.advance();
                let right = self.parse_additive_expression()?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(right),
                })
            }
            Token::Is => {
                self.advance();
                let negated = if self.current() == &Token::Not {
                    self.advance();
                    true
                } else {
                    false
                };
                self.expect(Token::Null)?;
                Ok(Expression::IsNull {
                    expr: Box::new(left),
                    negated,
                })
            }
            Token::In => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let list = self.parse_expression_list()?;
                self.expect(Token::RightParen)?;
                Ok(Expression::In {
                    expr: Box::new(left),
                    list,
                    negated: false,
                })
            }
            Token::Not => {
                self.advance();
                match self.current() {
                    Token::In => {
                        self.advance();
                        self.expect(Token::LeftParen)?;
                        let list = self.parse_expression_list()?;
                        self.expect(Token::RightParen)?;
                        Ok(Expression::In {
                            expr: Box::new(left),
                            list,
                            negated: true,
                        })
                    }
                    Token::Between => {
                        self.advance();
                        let low = self.parse_additive_expression()?;
                        self.expect(Token::And)?;
                        let high = self.parse_additive_expression()?;
                        Ok(Expression::Between {
                            expr: Box::new(left),
                            low: Box::new(low),
                            high: Box::new(high),
                            negated: true,
                        })
                    }
                    Token::Like => {
                        self.advance();
                        if let Token::StringLiteral(pattern) = self.current().clone() {
                            self.advance();
                            let escape = if self.current() == &Token::Escape {
                                self.advance();
                                if let Token::StringLiteral(esc) = self.current().clone() {
                                    self.advance();
                                    esc.chars().next()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            Ok(Expression::Like {
                                expr: Box::new(left),
                                pattern,
                                escape,
                                negated: true,
                            })
                        } else {
                            Err(StreamlineError::Parse(
                                "Expected string pattern after LIKE".to_string(),
                            ))
                        }
                    }
                    _ => Err(StreamlineError::Parse(
                        "Expected IN, BETWEEN, or LIKE after NOT".to_string(),
                    )),
                }
            }
            Token::Between => {
                self.advance();
                let low = self.parse_additive_expression()?;
                self.expect(Token::And)?;
                let high = self.parse_additive_expression()?;
                Ok(Expression::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                })
            }
            Token::Like => {
                self.advance();
                if let Token::StringLiteral(pattern) = self.current().clone() {
                    self.advance();
                    let escape = if self.current() == &Token::Escape {
                        self.advance();
                        if let Token::StringLiteral(esc) = self.current().clone() {
                            self.advance();
                            esc.chars().next()
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    Ok(Expression::Like {
                        expr: Box::new(left),
                        pattern,
                        escape,
                        negated: false,
                    })
                } else {
                    Err(StreamlineError::Parse(
                        "Expected string pattern after LIKE".to_string(),
                    ))
                }
            }
            _ => Ok(left),
        }
    }

    fn parse_additive_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_multiplicative_expression()?;

        loop {
            let op = match self.current() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Subtract,
                Token::Concat => BinaryOperator::Concat,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary_expression()?;

        loop {
            let op = match self.current() {
                Token::Asterisk => BinaryOperator::Multiply,
                Token::Slash => BinaryOperator::Divide,
                Token::Percent => BinaryOperator::Modulo,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expression(&mut self) -> Result<Expression> {
        match self.current() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Tilde => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::BitwiseNot,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expression(),
        }
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        match self.current().clone() {
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(Expression::Nested(Box::new(expr)))
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Literal(Literal::Null))
            }
            Token::True => {
                self.advance();
                Ok(Expression::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expression::Literal(Literal::Boolean(false)))
            }
            Token::IntegerLiteral(n) => {
                self.advance();
                Ok(Expression::Literal(Literal::Integer(n)))
            }
            Token::FloatLiteral(f) => {
                self.advance();
                Ok(Expression::Literal(Literal::Float(f)))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expression::Literal(Literal::String(s)))
            }
            Token::Identifier(name) => {
                self.advance();

                // Check for function call
                if self.current() == &Token::LeftParen {
                    self.advance();

                    let distinct = if self.current() == &Token::Distinct {
                        self.advance();
                        true
                    } else {
                        false
                    };

                    // Handle COUNT(*) and similar aggregate functions with *
                    let args = if self.current() == &Token::RightParen {
                        Vec::new()
                    } else if self.current() == &Token::Asterisk {
                        // * in a function like COUNT(*) - represent as Literal::Integer(1)
                        self.advance();
                        vec![Expression::Literal(Literal::Integer(1))]
                    } else {
                        self.parse_expression_list()?
                    };
                    self.expect(Token::RightParen)?;

                    // Check for OVER clause (window function)
                    if self.current() == &Token::Over {
                        self.advance();
                        self.expect(Token::LeftParen)?;
                        let (partition_by, order_by) = self.parse_window_clause()?;
                        self.expect(Token::RightParen)?;

                        Ok(Expression::WindowFunction {
                            function: Box::new(Expression::Function {
                                name,
                                args,
                                distinct,
                            }),
                            partition_by,
                            order_by,
                            frame: None,
                        })
                    } else {
                        Ok(Expression::Function {
                            name,
                            args,
                            distinct,
                        })
                    }
                } else if self.current() == &Token::Dot {
                    // Qualified column reference
                    self.advance();
                    let column = self.expect_identifier()?;
                    Ok(Expression::Column(ColumnRef::qualified(name, column)))
                } else {
                    // Simple column reference
                    Ok(Expression::Column(ColumnRef::simple(name)))
                }
            }
            Token::Case => self.parse_case_expression(),
            Token::Cast => self.parse_cast_expression(),
            _ => Err(StreamlineError::Parse(format!(
                "Unexpected token in expression: {:?}",
                self.current()
            ))),
        }
    }

    fn parse_case_expression(&mut self) -> Result<Expression> {
        self.expect(Token::Case)?;

        let operand = if self.current() != &Token::When {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.current() == &Token::When {
            self.advance();
            let condition = self.parse_expression()?;
            self.expect(Token::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push((condition, result));
        }

        let else_clause = if self.current() == &Token::Else {
            self.advance();
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(Token::End)?;

        Ok(Expression::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_cast_expression(&mut self) -> Result<Expression> {
        self.expect(Token::Cast)?;
        self.expect(Token::LeftParen)?;
        let expr = self.parse_expression()?;
        self.expect(Token::As)?;
        let data_type = self.parse_data_type()?;
        self.expect(Token::RightParen)?;

        Ok(Expression::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        match self.current() {
            Token::Identifier(name) => {
                let dt = match name.to_uppercase().as_str() {
                    "BOOLEAN" | "BOOL" => DataType::Boolean,
                    "INT" | "INTEGER" | "INT64" | "BIGINT" => DataType::Int64,
                    "FLOAT" | "DOUBLE" | "FLOAT64" => DataType::Float64,
                    "STRING" | "VARCHAR" | "TEXT" => DataType::String,
                    "BINARY" | "BYTES" | "BYTEA" => DataType::Binary,
                    "TIMESTAMP" | "DATETIME" => DataType::Timestamp,
                    "DATE" => DataType::Date,
                    "DURATION" | "INTERVAL" => DataType::Duration,
                    "JSON" | "JSONB" => DataType::Json,
                    _ => {
                        return Err(StreamlineError::Parse(format!(
                            "Unknown data type: {}",
                            name
                        )))
                    }
                };
                self.advance();
                Ok(dt)
            }
            Token::Null => {
                self.advance();
                Ok(DataType::Null)
            }
            _ => Err(StreamlineError::Parse("Expected data type".to_string())),
        }
    }

    fn parse_window_clause(&mut self) -> Result<(Vec<Expression>, Vec<OrderByItem>)> {
        let partition_by = if self.current() == &Token::Partition {
            self.advance();
            self.expect(Token::By)?;
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        let order_by = if self.current() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            self.parse_order_by_items()?
        } else {
            Vec::new()
        };

        Ok((partition_by, order_by))
    }

    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut exprs = Vec::new();

        if matches!(self.current(), Token::RightParen | Token::Eof) {
            return Ok(exprs);
        }

        loop {
            exprs.push(self.parse_expression()?);

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(exprs)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut idents = Vec::new();

        loop {
            idents.push(self.expect_identifier()?);

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(idents)
    }

    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let target = self.expect_identifier()?;

        let columns = if self.current() == &Token::LeftParen {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        let source = if self.current() == &Token::Select {
            InsertSource::Query(Box::new(self.parse_select()?))
        } else if self.current() == &Token::Values {
            self.advance();
            let mut rows = Vec::new();
            loop {
                self.expect(Token::LeftParen)?;
                let values = self.parse_expression_list()?;
                self.expect(Token::RightParen)?;
                rows.push(values);

                if self.current() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            InsertSource::Values(rows)
        } else {
            return Err(StreamlineError::Parse(
                "Expected SELECT or VALUES after INSERT".to_string(),
            ));
        };

        Ok(InsertStatement {
            target,
            columns,
            source,
        })
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;

        match self.current() {
            Token::Stream | Token::Table => {
                let _is_stream = self.current() == &Token::Stream;
                self.advance();

                let if_not_exists = if self.current() == &Token::If {
                    self.advance();
                    self.expect(Token::Not)?;
                    self.expect(Token::Exists)?;
                    true
                } else {
                    false
                };

                let name = self.expect_identifier()?;

                self.expect(Token::LeftParen)?;
                let columns = self.parse_column_defs()?;
                self.expect(Token::RightParen)?;

                let options = if self.current() == &Token::With {
                    self.advance();
                    self.expect(Token::LeftParen)?;
                    let opts = self.parse_with_options()?;
                    self.expect(Token::RightParen)?;
                    opts
                } else {
                    Vec::new()
                };

                Ok(Statement::CreateStream(CreateStreamStatement {
                    name,
                    columns,
                    options,
                    if_not_exists,
                }))
            }
            _ => Err(StreamlineError::Parse(
                "Expected STREAM or TABLE after CREATE".to_string(),
            )),
        }
    }

    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut cols = Vec::new();

        loop {
            let name = self.expect_identifier()?;
            let data_type = self.parse_data_type()?;

            let not_null = if self.current() == &Token::Not {
                self.advance();
                self.expect(Token::Null)?;
                true
            } else {
                false
            };

            cols.push(ColumnDef {
                name,
                data_type,
                not_null,
                default: None,
            });

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(cols)
    }

    fn parse_with_options(&mut self) -> Result<Vec<(String, String)>> {
        let mut opts = Vec::new();

        loop {
            let key = self.expect_identifier()?;
            self.expect(Token::Equal)?;

            let value = match self.current().clone() {
                Token::StringLiteral(s) => {
                    self.advance();
                    s
                }
                Token::IntegerLiteral(n) => {
                    self.advance();
                    n.to_string()
                }
                Token::Identifier(s) => {
                    self.advance();
                    s
                }
                _ => {
                    return Err(StreamlineError::Parse(
                        "Expected value in WITH option".to_string(),
                    ))
                }
            };

            opts.push((key, value));

            if self.current() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let stmt = StreamQLParser::parse("SELECT * FROM events").unwrap();
        if let Statement::Select(select) = stmt {
            assert_eq!(select.projections.len(), 1);
            assert!(matches!(select.projections[0], SelectItem::Wildcard));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_select_with_columns() {
        let stmt = StreamQLParser::parse("SELECT id, name, value FROM events").unwrap();
        if let Statement::Select(select) = stmt {
            assert_eq!(select.projections.len(), 3);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = StreamQLParser::parse("SELECT * FROM events WHERE id = 42").unwrap();
        if let Statement::Select(select) = stmt {
            assert!(select.filter.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_select_with_window() {
        let stmt = StreamQLParser::parse(
            "SELECT user_id, COUNT(*) FROM events WINDOW TUMBLING('1 minute') GROUP BY user_id",
        )
        .unwrap();
        if let Statement::Select(select) = stmt {
            assert!(select.window.is_some());
            assert!(select.group_by.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_aggregates() {
        let stmt =
            StreamQLParser::parse("SELECT COUNT(*), SUM(value), AVG(price) FROM orders").unwrap();
        if let Statement::Select(select) = stmt {
            assert_eq!(select.projections.len(), 3);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_parse_order_by_limit() {
        let stmt = StreamQLParser::parse("SELECT * FROM events ORDER BY timestamp DESC LIMIT 100")
            .unwrap();
        if let Statement::Select(select) = stmt {
            assert!(select.order_by.is_some());
            assert_eq!(select.limit, Some(100));
        } else {
            panic!("Expected SELECT statement");
        }
    }
}
