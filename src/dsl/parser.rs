//! DSL Parser
//!
//! Parses SQL-like stream processing queries into pipeline definitions.
//!
//! # Syntax
//!
//! ```text
//! SELECT <fields>
//! FROM STREAM '<topic>'
//! [WHERE <condition>]
//! [WINDOW <window_spec>]
//! [GROUP BY <fields>]
//! [HAVING <condition>]
//! [EMIT TO '<topic>']
//! ```

use super::operators::{AggregateFunction, BinaryOperator, Expression};
use super::window::WindowType;
use serde::{Deserialize, Serialize};

/// Parse errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParseError {
    /// Unexpected token
    UnexpectedToken { expected: String, found: String },
    /// Unexpected end of input
    UnexpectedEnd,
    /// Invalid syntax
    InvalidSyntax(String),
    /// Unknown function
    UnknownFunction(String),
    /// Invalid window specification
    InvalidWindow(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedToken { expected, found } => {
                write!(f, "Expected {}, found {}", expected, found)
            }
            ParseError::UnexpectedEnd => write!(f, "Unexpected end of input"),
            ParseError::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
            ParseError::UnknownFunction(name) => write!(f, "Unknown function: {}", name),
            ParseError::InvalidWindow(msg) => write!(f, "Invalid window: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

/// Result type for parsing
pub type ParseResult<T> = Result<T, ParseError>;

/// Stream source specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSource {
    /// Source topic name
    pub topic: String,
    /// Optional alias
    pub alias: Option<String>,
}

/// Select field specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectField {
    /// All fields (*)
    All,
    /// Single field
    Field { name: String, alias: Option<String> },
    /// Aggregate function
    Aggregate {
        function: AggregateFunction,
        field: Option<String>,
        alias: String,
    },
    /// Expression
    Expression { expr: Expression, alias: String },
}

/// Parsed query representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    /// Selected fields
    pub select: Vec<SelectField>,
    /// Source stream
    pub from: StreamSource,
    /// Optional join
    pub join: Option<JoinSpec>,
    /// Where clause
    pub filter: Option<Expression>,
    /// Window specification
    pub window: Option<WindowType>,
    /// Group by fields
    pub group_by: Vec<String>,
    /// Having clause
    pub having: Option<Expression>,
    /// Output topic
    pub emit_to: Option<String>,
}

/// Join specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Join type
    pub join_type: JoinType,
    /// Right stream
    pub stream: StreamSource,
    /// Join condition
    pub on: Expression,
    /// Window for join
    pub within: Option<u64>,
}

/// Join type
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// DSL Parser
pub struct DslParser {
    tokens: Vec<Token>,
    pos: usize,
}

/// Token types
#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Keywords
    Select,
    From,
    Stream,
    Where,
    Window,
    GroupBy,
    Having,
    EmitTo,
    As,
    And,
    Or,
    Not,
    In,
    Like,
    Is,
    Null,
    Join,
    Inner,
    Left,
    Right,
    Full,
    On,
    Within,
    Tumbling,
    Sliding,
    Session,
    Minute,
    Minutes,
    Hour,
    Hours,
    Second,
    Seconds,
    Day,
    Days,

    // Literals
    Identifier(String),
    StringLit(String),
    NumberLit(f64),
    True,
    False,

    // Operators
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Slash,
    Percent,

    // End
    Eof,
}

impl DslParser {
    /// Parse a SQL-like query string
    pub fn parse(query: &str) -> ParseResult<ParsedQuery> {
        let tokens = Self::tokenize(query)?;
        let mut parser = Self { tokens, pos: 0 };
        parser.parse_query()
    }

    /// Tokenize input string
    fn tokenize(input: &str) -> ParseResult<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut chars = input.chars().peekable();

        while let Some(&ch) = chars.peek() {
            match ch {
                ' ' | '\t' | '\n' | '\r' => {
                    chars.next();
                }
                '*' => {
                    chars.next();
                    tokens.push(Token::Star);
                }
                ',' => {
                    chars.next();
                    tokens.push(Token::Comma);
                }
                '.' => {
                    chars.next();
                    tokens.push(Token::Dot);
                }
                '(' => {
                    chars.next();
                    tokens.push(Token::LParen);
                }
                ')' => {
                    chars.next();
                    tokens.push(Token::RParen);
                }
                '+' => {
                    chars.next();
                    tokens.push(Token::Plus);
                }
                '-' => {
                    chars.next();
                    if chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                        // Negative number
                        let num = Self::read_number(&mut chars, true);
                        tokens.push(Token::NumberLit(num));
                    } else {
                        tokens.push(Token::Minus);
                    }
                }
                '/' => {
                    chars.next();
                    tokens.push(Token::Slash);
                }
                '%' => {
                    chars.next();
                    tokens.push(Token::Percent);
                }
                '=' => {
                    chars.next();
                    tokens.push(Token::Eq);
                }
                '!' => {
                    chars.next();
                    if chars.peek() == Some(&'=') {
                        chars.next();
                        tokens.push(Token::Ne);
                    } else {
                        return Err(ParseError::InvalidSyntax(
                            "Expected '=' after '!'".to_string(),
                        ));
                    }
                }
                '<' => {
                    chars.next();
                    if chars.peek() == Some(&'=') {
                        chars.next();
                        tokens.push(Token::Le);
                    } else if chars.peek() == Some(&'>') {
                        chars.next();
                        tokens.push(Token::Ne);
                    } else {
                        tokens.push(Token::Lt);
                    }
                }
                '>' => {
                    chars.next();
                    if chars.peek() == Some(&'=') {
                        chars.next();
                        tokens.push(Token::Ge);
                    } else {
                        tokens.push(Token::Gt);
                    }
                }
                '\'' | '"' => {
                    let quote = chars.next().ok_or(ParseError::UnexpectedEnd)?;
                    let mut s = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == quote {
                            chars.next();
                            break;
                        }
                        s.push(chars.next().ok_or(ParseError::UnexpectedEnd)?);
                    }
                    tokens.push(Token::StringLit(s));
                }
                c if c.is_ascii_digit() => {
                    let num = Self::read_number(&mut chars, false);
                    tokens.push(Token::NumberLit(num));
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    let mut ident = String::new();
                    while let Some(&c) = chars.peek() {
                        if c.is_ascii_alphanumeric() || c == '_' {
                            ident.push(chars.next().ok_or(ParseError::UnexpectedEnd)?);
                        } else {
                            break;
                        }
                    }
                    let token = Self::keyword_or_ident(&ident);
                    tokens.push(token);
                }
                _ => {
                    return Err(ParseError::InvalidSyntax(format!(
                        "Unexpected character: {}",
                        ch
                    )));
                }
            }
        }

        tokens.push(Token::Eof);
        Ok(tokens)
    }

    fn read_number(chars: &mut std::iter::Peekable<std::str::Chars>, negative: bool) -> f64 {
        let mut num_str = if negative {
            "-".to_string()
        } else {
            String::new()
        };
        let mut has_dot = false;

        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                if let Some(c) = chars.next() {
                    num_str.push(c);
                }
            } else if c == '.' && !has_dot {
                has_dot = true;
                if let Some(c) = chars.next() {
                    num_str.push(c);
                }
            } else {
                break;
            }
        }

        num_str.parse().unwrap_or(0.0)
    }

    fn keyword_or_ident(s: &str) -> Token {
        match s.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "STREAM" => Token::Stream,
            "WHERE" => Token::Where,
            "WINDOW" => Token::Window,
            "GROUP" => Token::GroupBy, // Will handle "BY" separately
            "HAVING" => Token::Having,
            "EMIT" => Token::EmitTo, // Will handle "TO" separately
            "AS" => Token::As,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "IN" => Token::In,
            "LIKE" => Token::Like,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "ON" => Token::On,
            "WITHIN" => Token::Within,
            "TUMBLING" => Token::Tumbling,
            "SLIDING" => Token::Sliding,
            "SESSION" => Token::Session,
            "MINUTE" => Token::Minute,
            "MINUTES" => Token::Minutes,
            "HOUR" => Token::Hour,
            "HOURS" => Token::Hours,
            "SECOND" => Token::Second,
            "SECONDS" => Token::Seconds,
            "DAY" => Token::Day,
            "DAYS" => Token::Days,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "BY" => Token::Identifier("BY".to_string()), // Handle in parser
            "TO" => Token::Identifier("TO".to_string()), // Handle in parser
            _ => Token::Identifier(s.to_string()),
        }
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let _token = self.current().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        self.tokens.get(self.pos - 1).unwrap_or(&Token::Eof)
    }

    fn expect(&mut self, expected: &Token) -> ParseResult<()> {
        if std::mem::discriminant(self.current()) == std::mem::discriminant(expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                expected: format!("{:?}", expected),
                found: format!("{:?}", self.current()),
            })
        }
    }

    fn parse_query(&mut self) -> ParseResult<ParsedQuery> {
        // SELECT
        self.expect(&Token::Select)?;
        let select = self.parse_select_list()?;

        // FROM STREAM 'topic'
        self.expect(&Token::From)?;
        self.expect(&Token::Stream)?;
        let from = self.parse_stream_source()?;

        // Optional JOIN
        let join = self.parse_optional_join()?;

        // Optional WHERE
        let filter = if matches!(self.current(), Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Optional WINDOW
        let window = if matches!(self.current(), Token::Window) {
            self.advance();
            Some(self.parse_window_spec()?)
        } else {
            None
        };

        // Optional GROUP BY
        let group_by = if matches!(self.current(), Token::GroupBy) {
            self.advance();
            // Skip "BY" if present
            if let Token::Identifier(s) = self.current() {
                if s.to_uppercase() == "BY" {
                    self.advance();
                }
            }
            self.parse_identifier_list()?
        } else {
            vec![]
        };

        // Optional HAVING
        let having = if matches!(self.current(), Token::Having) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Optional EMIT TO 'topic'
        let emit_to = if matches!(self.current(), Token::EmitTo) {
            self.advance();
            // Skip "TO" if present
            if let Token::Identifier(s) = self.current() {
                if s.to_uppercase() == "TO" {
                    self.advance();
                }
            }
            if let Token::StringLit(topic) = self.current().clone() {
                self.advance();
                Some(topic)
            } else {
                return Err(ParseError::UnexpectedToken {
                    expected: "string literal".to_string(),
                    found: format!("{:?}", self.current()),
                });
            }
        } else {
            None
        };

        Ok(ParsedQuery {
            select,
            from,
            join,
            filter,
            window,
            group_by,
            having,
            emit_to,
        })
    }

    fn parse_select_list(&mut self) -> ParseResult<Vec<SelectField>> {
        let mut fields = Vec::new();

        loop {
            let field = self.parse_select_field()?;
            fields.push(field);

            if matches!(self.current(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(fields)
    }

    fn parse_select_field(&mut self) -> ParseResult<SelectField> {
        if matches!(self.current(), Token::Star) {
            self.advance();
            return Ok(SelectField::All);
        }

        // Check for aggregate function (must have parentheses following)
        if let Token::Identifier(name) = self.current().clone() {
            if let Some(func) = Self::parse_aggregate_name(&name) {
                // Look ahead to check if this is actually a function call
                let next_is_lparen = self.tokens.get(self.pos + 1) == Some(&Token::LParen);
                if next_is_lparen {
                    self.advance();
                    self.expect(&Token::LParen)?;

                    let field = if matches!(self.current(), Token::Star) {
                        self.advance();
                        None
                    } else if let Token::Identifier(f) = self.current().clone() {
                        self.advance();
                        Some(f)
                    } else {
                        None
                    };

                    self.expect(&Token::RParen)?;

                    let alias = self.parse_optional_alias()?.unwrap_or_else(|| {
                        format!(
                            "{}_{}",
                            name.to_lowercase(),
                            field.as_deref().unwrap_or("all")
                        )
                    });

                    return Ok(SelectField::Aggregate {
                        function: func,
                        field,
                        alias,
                    });
                }
                // Otherwise, fall through to treat it as a regular field
            }
        }

        // Regular field
        if let Token::Identifier(name) = self.current().clone() {
            self.advance();
            let alias = self.parse_optional_alias()?;
            Ok(SelectField::Field { name, alias })
        } else {
            Err(ParseError::UnexpectedToken {
                expected: "field name".to_string(),
                found: format!("{:?}", self.current()),
            })
        }
    }

    fn parse_aggregate_name(name: &str) -> Option<AggregateFunction> {
        match name.to_uppercase().as_str() {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "FIRST" => Some(AggregateFunction::First),
            "LAST" => Some(AggregateFunction::Last),
            "COUNT_DISTINCT" => Some(AggregateFunction::CountDistinct),
            _ => None,
        }
    }

    fn parse_optional_alias(&mut self) -> ParseResult<Option<String>> {
        if matches!(self.current(), Token::As) {
            self.advance();
            if let Token::Identifier(alias) = self.current().clone() {
                self.advance();
                Ok(Some(alias))
            } else {
                Err(ParseError::UnexpectedToken {
                    expected: "alias".to_string(),
                    found: format!("{:?}", self.current()),
                })
            }
        } else {
            Ok(None)
        }
    }

    fn parse_stream_source(&mut self) -> ParseResult<StreamSource> {
        let topic = if let Token::StringLit(t) = self.current().clone() {
            self.advance();
            t
        } else {
            return Err(ParseError::UnexpectedToken {
                expected: "topic name".to_string(),
                found: format!("{:?}", self.current()),
            });
        };

        let alias = self.parse_optional_alias()?;

        Ok(StreamSource { topic, alias })
    }

    fn parse_optional_join(&mut self) -> ParseResult<Option<JoinSpec>> {
        let join_type = match self.current() {
            Token::Join => {
                self.advance();
                JoinType::Inner
            }
            Token::Inner => {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::Inner
            }
            Token::Left => {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::Left
            }
            Token::Right => {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::Right
            }
            Token::Full => {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::Full
            }
            _ => return Ok(None),
        };

        self.expect(&Token::Stream)?;
        let stream = self.parse_stream_source()?;

        self.expect(&Token::On)?;
        let on = self.parse_expression()?;

        let within = if matches!(self.current(), Token::Within) {
            self.advance();
            Some(self.parse_duration()?)
        } else {
            None
        };

        Ok(Some(JoinSpec {
            join_type,
            stream,
            on,
            within,
        }))
    }

    fn parse_window_spec(&mut self) -> ParseResult<WindowType> {
        match self.current() {
            Token::Tumbling => {
                self.advance();
                let duration_ms = self.parse_duration()?;
                Ok(WindowType::Tumbling { duration_ms })
            }
            Token::Sliding => {
                self.advance();
                let size_ms = self.parse_duration()?;

                // Expect comma or slide specification
                if matches!(self.current(), Token::Comma) {
                    self.advance();
                }

                let slide_ms = self.parse_duration()?;
                Ok(WindowType::Sliding { size_ms, slide_ms })
            }
            Token::Session => {
                self.advance();
                let gap_ms = self.parse_duration()?;
                Ok(WindowType::Session {
                    gap_ms,
                    max_duration_ms: None,
                })
            }
            _ => Err(ParseError::InvalidWindow(format!(
                "Expected TUMBLING, SLIDING, or SESSION, found {:?}",
                self.current()
            ))),
        }
    }

    fn parse_duration(&mut self) -> ParseResult<u64> {
        let value = if let Token::NumberLit(n) = self.current().clone() {
            self.advance();
            n as u64
        } else {
            return Err(ParseError::UnexpectedToken {
                expected: "number".to_string(),
                found: format!("{:?}", self.current()),
            });
        };

        let multiplier = match self.current() {
            Token::Second | Token::Seconds => {
                self.advance();
                1000
            }
            Token::Minute | Token::Minutes => {
                self.advance();
                60 * 1000
            }
            Token::Hour | Token::Hours => {
                self.advance();
                60 * 60 * 1000
            }
            Token::Day | Token::Days => {
                self.advance();
                24 * 60 * 60 * 1000
            }
            _ => 1, // Assume milliseconds
        };

        Ok(value * multiplier)
    }

    fn parse_identifier_list(&mut self) -> ParseResult<Vec<String>> {
        let mut idents = Vec::new();

        while let Token::Identifier(name) = self.current().clone() {
            self.advance();
            idents.push(name);

            if matches!(self.current(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(idents)
    }

    fn parse_expression(&mut self) -> ParseResult<Expression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> ParseResult<Expression> {
        let mut left = self.parse_and_expression()?;

        while matches!(self.current(), Token::Or) {
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

    fn parse_and_expression(&mut self) -> ParseResult<Expression> {
        let mut left = self.parse_comparison()?;

        while matches!(self.current(), Token::And) {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> ParseResult<Expression> {
        let left = self.parse_additive()?;

        let op = match self.current() {
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Ne => Some(BinaryOperator::Ne),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::Le => Some(BinaryOperator::Le),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::Ge => Some(BinaryOperator::Ge),
            Token::Like => Some(BinaryOperator::Like),
            Token::In => Some(BinaryOperator::In),
            _ => None,
        };

        if let Some(operator) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expression::BinaryOp {
                left: Box::new(left),
                op: operator,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_additive(&mut self) -> ParseResult<Expression> {
        let mut left = self.parse_multiplicative()?;

        loop {
            let op = match self.current() {
                Token::Plus => Some(BinaryOperator::Add),
                Token::Minus => Some(BinaryOperator::Sub),
                _ => None,
            };

            if let Some(operator) = op {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: operator,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> ParseResult<Expression> {
        let mut left = self.parse_unary()?;

        loop {
            let op = match self.current() {
                Token::Star => Some(BinaryOperator::Mul),
                Token::Slash => Some(BinaryOperator::Div),
                Token::Percent => Some(BinaryOperator::Mod),
                _ => None,
            };

            if let Some(operator) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: operator,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_unary(&mut self) -> ParseResult<Expression> {
        if matches!(self.current(), Token::Not) {
            self.advance();
            let expr = self.parse_unary()?;
            Ok(Expression::UnaryOp {
                op: super::operators::UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else if matches!(self.current(), Token::Minus) {
            self.advance();
            let expr = self.parse_unary()?;
            Ok(Expression::UnaryOp {
                op: super::operators::UnaryOperator::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> ParseResult<Expression> {
        match self.current().clone() {
            Token::NumberLit(n) => {
                self.advance();
                Ok(Expression::Literal(serde_json::json!(n)))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expression::Literal(serde_json::Value::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expression::Literal(serde_json::Value::Bool(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expression::Literal(serde_json::Value::Bool(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Literal(serde_json::Value::Null))
            }
            Token::Identifier(name) => {
                self.advance();

                // Check for function call
                if matches!(self.current(), Token::LParen) {
                    self.advance();
                    let args = self.parse_function_args()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expression::Function { name, args })
                } else {
                    Ok(Expression::Field(name))
                }
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "expression".to_string(),
                found: format!("{:?}", self.current()),
            }),
        }
    }

    fn parse_function_args(&mut self) -> ParseResult<Vec<Expression>> {
        let mut args = Vec::new();

        if !matches!(self.current(), Token::RParen) {
            loop {
                args.push(self.parse_expression()?);
                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        Ok(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = "SELECT * FROM STREAM 'events'";
        let parsed = DslParser::parse(query).unwrap();

        assert_eq!(parsed.from.topic, "events");
        assert!(matches!(parsed.select[0], SelectField::All));
    }

    #[test]
    fn test_parse_select_with_fields() {
        let query = "SELECT user_id, count FROM STREAM 'events'";
        let parsed = DslParser::parse(query).unwrap();

        assert_eq!(parsed.select.len(), 2);
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = "SELECT * FROM STREAM 'events' WHERE type = 'click'";
        let parsed = DslParser::parse(query).unwrap();

        assert!(parsed.filter.is_some());
    }

    #[test]
    fn test_parse_aggregate() {
        let query = "SELECT user_id, COUNT(*) AS total FROM STREAM 'events' GROUP BY user_id";
        let parsed = DslParser::parse(query).unwrap();

        assert_eq!(parsed.group_by, vec!["user_id"]);
        assert!(matches!(
            parsed.select[1],
            SelectField::Aggregate {
                function: AggregateFunction::Count,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_window() {
        let query = "SELECT * FROM STREAM 'events' WINDOW TUMBLING 5 MINUTES";
        let parsed = DslParser::parse(query).unwrap();

        assert!(matches!(
            parsed.window,
            Some(WindowType::Tumbling {
                duration_ms: 300000
            })
        ));
    }

    #[test]
    fn test_parse_sliding_window() {
        let query = "SELECT * FROM STREAM 'events' WINDOW SLIDING 10 MINUTES, 1 MINUTE";
        let parsed = DslParser::parse(query).unwrap();

        assert!(matches!(
            parsed.window,
            Some(WindowType::Sliding {
                size_ms: 600000,
                slide_ms: 60000
            })
        ));
    }

    #[test]
    fn test_parse_emit_to() {
        let query = "SELECT * FROM STREAM 'events' EMIT TO 'output'";
        let parsed = DslParser::parse(query).unwrap();

        assert_eq!(parsed.emit_to, Some("output".to_string()));
    }

    #[test]
    fn test_parse_complex_where() {
        let query = "SELECT * FROM STREAM 'events' WHERE age > 18 AND status = 'active'";
        let parsed = DslParser::parse(query).unwrap();

        assert!(parsed.filter.is_some());
    }

    #[test]
    fn test_parse_full_query() {
        let query = r#"
            SELECT user_id, COUNT(*) AS click_count
            FROM STREAM 'events'
            WHERE type = 'click'
            WINDOW TUMBLING 1 HOUR
            GROUP BY user_id
            HAVING click_count > 10
            EMIT TO 'aggregated'
        "#;

        let parsed = DslParser::parse(query).unwrap();

        assert_eq!(parsed.from.topic, "events");
        assert!(parsed.filter.is_some());
        assert!(parsed.window.is_some());
        assert_eq!(parsed.group_by, vec!["user_id"]);
        assert!(parsed.having.is_some());
        assert_eq!(parsed.emit_to, Some("aggregated".to_string()));
    }
}
