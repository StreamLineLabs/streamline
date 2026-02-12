//! Lexer for StreamlineQL.
//!
//! Tokenizes SQL-like query strings into a stream of tokens for parsing.
//!
//! # Token Types
//!
//! The lexer recognizes:
//! - **Keywords**: SQL keywords like `SELECT`, `FROM`, `WHERE`, `GROUP BY`
//! - **Identifiers**: Unquoted names like `table_name`, `column_name`
//! - **Quoted Identifiers**: Double-quoted names like `"My Column"`
//! - **String Literals**: Single-quoted strings like `'hello world'`
//! - **Numeric Literals**: Integers (`42`) and floats (`3.14`, `1e10`)
//! - **Operators**: Arithmetic (`+`, `-`, `*`, `/`), comparison (`=`, `<>`, `<`, `>`), logical (`AND`, `OR`)
//! - **Punctuation**: Parentheses, commas, semicolons
//!
//! # Example
//!
//! ```ignore
//! use streamline::streamql::lexer::{Lexer, Token};
//!
//! let mut lexer = Lexer::new("SELECT * FROM events WHERE id = 42");
//! let tokens = lexer.tokenize().unwrap();
//!
//! assert_eq!(tokens[0], Token::Select);
//! assert_eq!(tokens[1], Token::Asterisk);
//! assert_eq!(tokens[2], Token::From);
//! ```
//!
//! # Comments
//!
//! The lexer supports two comment styles:
//! - Line comments: `-- comment until end of line`
//! - Block comments: `/* multi-line comment */` (with nesting support)

use crate::error::{Result, StreamlineError};
use std::str::CharIndices;

/// Token types in StreamlineQL
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    As,
    And,
    Or,
    Not,
    In,
    Is,
    Null,
    True,
    False,
    Between,
    Like,
    Escape,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    Distinct,
    All,
    Limit,
    Offset,
    Order,
    By,
    Asc,
    Desc,
    Nulls,
    First,
    Last,
    Group,
    Having,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Cross,
    On,
    Using,
    Natural,
    Window,
    Tumbling,
    Hopping,
    Sliding,
    Session,
    Interval,
    Partition,
    Rows,
    Range,
    Unbounded,
    Preceding,
    Following,
    Current,
    Row,
    Over,
    Insert,
    Into,
    Values,
    Create,
    Stream,
    Table,
    If,
    Exists,
    With,
    Explain,
    Union,
    Except,
    Intersect,

    // Identifiers and literals
    Identifier(String),
    QuotedIdentifier(String),
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),

    // Operators
    Plus,          // +
    Minus,         // -
    Asterisk,      // *
    Slash,         // /
    Percent,       // %
    Equal,         // =
    NotEqual,      // <> or !=
    LessThan,      // <
    LessThanEq,    // <=
    GreaterThan,   // >
    GreaterThanEq, // >=
    Concat,        // ||
    Ampersand,     // &
    Pipe,          // |
    Caret,         // ^
    Tilde,         // ~

    // Punctuation
    LeftParen,    // (
    RightParen,   // )
    LeftBracket,  // [
    RightBracket, // ]
    Comma,        // ,
    Semicolon,    // ;
    Colon,        // :
    DoubleColon,  // ::
    Dot,          // .

    // Special
    Eof,
}

impl Token {
    /// Check if this token is a keyword
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            Token::Select
                | Token::From
                | Token::Where
                | Token::As
                | Token::And
                | Token::Or
                | Token::Not
                | Token::In
                | Token::Is
                | Token::Null
                | Token::True
                | Token::False
                | Token::Between
                | Token::Like
                | Token::Case
                | Token::When
                | Token::Then
                | Token::Else
                | Token::End
                | Token::Cast
                | Token::Distinct
                | Token::All
                | Token::Limit
                | Token::Offset
                | Token::Order
                | Token::By
                | Token::Group
                | Token::Having
                | Token::Join
                | Token::Window
        )
    }
}

/// Lexer for StreamlineQL
pub struct Lexer<'a> {
    /// Input string
    input: &'a str,
    /// Character iterator
    chars: CharIndices<'a>,
    /// Current character and position
    current: Option<(usize, char)>,
    /// Peeked character and position
    peeked: Option<(usize, char)>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given input
    pub fn new(input: &'a str) -> Self {
        let mut chars = input.char_indices();
        let current = chars.next();
        let peeked = chars.next();
        Self {
            input,
            chars,
            current,
            peeked,
        }
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();

        match self.current {
            None => Ok(Token::Eof),
            Some((_, c)) => match c {
                // Single-character tokens
                '(' => self.single_char_token(Token::LeftParen),
                ')' => self.single_char_token(Token::RightParen),
                '[' => self.single_char_token(Token::LeftBracket),
                ']' => self.single_char_token(Token::RightBracket),
                ',' => self.single_char_token(Token::Comma),
                ';' => self.single_char_token(Token::Semicolon),
                '+' => self.single_char_token(Token::Plus),
                '-' => {
                    if self.peek_char() == Some('-') {
                        // Line comment
                        self.skip_line_comment();
                        self.next_token()
                    } else {
                        self.single_char_token(Token::Minus)
                    }
                }
                '*' => self.single_char_token(Token::Asterisk),
                '/' => {
                    if self.peek_char() == Some('*') {
                        // Block comment
                        self.skip_block_comment()?;
                        self.next_token()
                    } else {
                        self.single_char_token(Token::Slash)
                    }
                }
                '%' => self.single_char_token(Token::Percent),
                '&' => self.single_char_token(Token::Ampersand),
                '^' => self.single_char_token(Token::Caret),
                '~' => self.single_char_token(Token::Tilde),
                '.' => self.single_char_token(Token::Dot),

                // Multi-character operators
                '=' => self.single_char_token(Token::Equal),
                '<' => self.less_than_operator(),
                '>' => self.greater_than_operator(),
                '!' => self.not_equal_operator(),
                '|' => self.pipe_operator(),
                ':' => self.colon_operator(),

                // String literals
                '\'' => self.string_literal(),

                // Quoted identifiers
                '"' => self.quoted_identifier(),

                // Numbers
                c if c.is_ascii_digit() => self.number_literal(),

                // Identifiers and keywords
                c if c.is_alphabetic() || c == '_' => self.identifier_or_keyword(),

                _ => Err(StreamlineError::Parse(format!(
                    "Unexpected character: '{}'",
                    c
                ))),
            },
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn advance(&mut self) {
        self.current = self.peeked.take();
        self.peeked = self.chars.next();
    }

    fn peek_char(&self) -> Option<char> {
        self.peeked.map(|(_, c)| c)
    }

    fn skip_whitespace_and_comments(&mut self) {
        while let Some((_, c)) = self.current {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some((_, c)) = self.current {
            self.advance();
            if c == '\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<()> {
        self.advance(); // Skip /
        self.advance(); // Skip *
        let mut depth = 1;

        while let Some((_, c)) = self.current {
            if c == '*' && self.peek_char() == Some('/') {
                self.advance();
                self.advance();
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            } else if c == '/' && self.peek_char() == Some('*') {
                self.advance();
                self.advance();
                depth += 1;
            } else {
                self.advance();
            }
        }

        Err(StreamlineError::Parse(
            "Unterminated block comment".to_string(),
        ))
    }

    fn single_char_token(&mut self, token: Token) -> Result<Token> {
        self.advance();
        Ok(token)
    }

    fn less_than_operator(&mut self) -> Result<Token> {
        self.advance();
        match self.current.map(|(_, c)| c) {
            Some('=') => {
                self.advance();
                Ok(Token::LessThanEq)
            }
            Some('>') => {
                self.advance();
                Ok(Token::NotEqual)
            }
            _ => Ok(Token::LessThan),
        }
    }

    fn greater_than_operator(&mut self) -> Result<Token> {
        self.advance();
        if self.current.map(|(_, c)| c) == Some('=') {
            self.advance();
            Ok(Token::GreaterThanEq)
        } else {
            Ok(Token::GreaterThan)
        }
    }

    fn not_equal_operator(&mut self) -> Result<Token> {
        self.advance();
        if self.current.map(|(_, c)| c) == Some('=') {
            self.advance();
            Ok(Token::NotEqual)
        } else {
            Err(StreamlineError::Parse("Expected '=' after '!'".to_string()))
        }
    }

    fn pipe_operator(&mut self) -> Result<Token> {
        self.advance();
        if self.current.map(|(_, c)| c) == Some('|') {
            self.advance();
            Ok(Token::Concat)
        } else {
            Ok(Token::Pipe)
        }
    }

    fn colon_operator(&mut self) -> Result<Token> {
        self.advance();
        if self.current.map(|(_, c)| c) == Some(':') {
            self.advance();
            Ok(Token::DoubleColon)
        } else {
            Ok(Token::Colon)
        }
    }

    fn string_literal(&mut self) -> Result<Token> {
        self.advance(); // Skip opening quote
        let start = self.current.map(|(i, _)| i).unwrap_or(self.input.len());
        let mut value = String::new();

        while let Some((_, c)) = self.current {
            if c == '\'' {
                // Check for escaped quote ('')
                if self.peek_char() == Some('\'') {
                    value.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // Skip closing quote
                    return Ok(Token::StringLiteral(value));
                }
            } else {
                value.push(c);
                self.advance();
            }
        }

        Err(StreamlineError::Parse(format!(
            "Unterminated string literal starting at position {}",
            start
        )))
    }

    fn quoted_identifier(&mut self) -> Result<Token> {
        self.advance(); // Skip opening quote
        let start = self.current.map(|(i, _)| i).unwrap_or(self.input.len());
        let mut value = String::new();

        while let Some((_, c)) = self.current {
            if c == '"' {
                // Check for escaped quote ("")
                if self.peek_char() == Some('"') {
                    value.push('"');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // Skip closing quote
                    return Ok(Token::QuotedIdentifier(value));
                }
            } else {
                value.push(c);
                self.advance();
            }
        }

        Err(StreamlineError::Parse(format!(
            "Unterminated quoted identifier starting at position {}",
            start
        )))
    }

    fn number_literal(&mut self) -> Result<Token> {
        let start = self.current.map(|(i, _)| i).unwrap_or(0);
        let mut is_float = false;

        while let Some((pos, c)) = self.current {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == '.' && !is_float {
                is_float = true;
                self.advance();
            } else if (c == 'e' || c == 'E') && !is_float {
                is_float = true;
                self.advance();
                // Handle optional sign after exponent
                if self.current.map(|(_, c)| c) == Some('+')
                    || self.current.map(|(_, c)| c) == Some('-')
                {
                    self.advance();
                }
            } else {
                let end = pos;
                let value = &self.input[start..end];
                return if is_float {
                    value
                        .parse::<f64>()
                        .map(Token::FloatLiteral)
                        .map_err(|e| StreamlineError::Parse(format!("Invalid float: {}", e)))
                } else {
                    value
                        .parse::<i64>()
                        .map(Token::IntegerLiteral)
                        .map_err(|e| StreamlineError::Parse(format!("Invalid integer: {}", e)))
                };
            }
        }

        // End of input
        let value = &self.input[start..];
        if is_float {
            value
                .parse::<f64>()
                .map(Token::FloatLiteral)
                .map_err(|e| StreamlineError::Parse(format!("Invalid float: {}", e)))
        } else {
            value
                .parse::<i64>()
                .map(Token::IntegerLiteral)
                .map_err(|e| StreamlineError::Parse(format!("Invalid integer: {}", e)))
        }
    }

    fn identifier_or_keyword(&mut self) -> Result<Token> {
        let start = self.current.map(|(i, _)| i).unwrap_or(0);

        while let Some((pos, c)) = self.current {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                let value = &self.input[start..pos];
                return Ok(self.keyword_or_identifier(value));
            }
        }

        // End of input
        let value = &self.input[start..];
        Ok(self.keyword_or_identifier(value))
    }

    fn keyword_or_identifier(&self, value: &str) -> Token {
        // Case-insensitive keyword matching
        match value.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "AS" => Token::As,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "IN" => Token::In,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "BETWEEN" => Token::Between,
            "LIKE" => Token::Like,
            "ESCAPE" => Token::Escape,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "CAST" => Token::Cast,
            "DISTINCT" => Token::Distinct,
            "ALL" => Token::All,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "NULLS" => Token::Nulls,
            "FIRST" => Token::First,
            "LAST" => Token::Last,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "OUTER" => Token::Outer,
            "CROSS" => Token::Cross,
            "ON" => Token::On,
            "USING" => Token::Using,
            "NATURAL" => Token::Natural,
            "WINDOW" => Token::Window,
            "TUMBLING" => Token::Tumbling,
            "HOPPING" => Token::Hopping,
            "SLIDING" => Token::Sliding,
            "SESSION" => Token::Session,
            "INTERVAL" => Token::Interval,
            "PARTITION" => Token::Partition,
            "ROWS" => Token::Rows,
            "RANGE" => Token::Range,
            "UNBOUNDED" => Token::Unbounded,
            "PRECEDING" => Token::Preceding,
            "FOLLOWING" => Token::Following,
            "CURRENT" => Token::Current,
            "ROW" => Token::Row,
            "OVER" => Token::Over,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "STREAM" => Token::Stream,
            "TABLE" => Token::Table,
            "IF" => Token::If,
            "EXISTS" => Token::Exists,
            "WITH" => Token::With,
            "EXPLAIN" => Token::Explain,
            "UNION" => Token::Union,
            "EXCEPT" => Token::Except,
            "INTERSECT" => Token::Intersect,
            _ => Token::Identifier(value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM events");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Asterisk,
                Token::From,
                Token::Identifier("events".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_lexer_select_with_where() {
        let mut lexer = Lexer::new("SELECT id, name FROM users WHERE id = 42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Identifier("id".to_string()),
                Token::Comma,
                Token::Identifier("name".to_string()),
                Token::From,
                Token::Identifier("users".to_string()),
                Token::Where,
                Token::Identifier("id".to_string()),
                Token::Equal,
                Token::IntegerLiteral(42),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_lexer_string_literal() {
        let mut lexer = Lexer::new("'hello world'");
        let token = lexer.next_token().unwrap();
        assert_eq!(token, Token::StringLiteral("hello world".to_string()));
    }

    #[test]
    fn test_lexer_operators() {
        let mut lexer = Lexer::new("<= >= <> != ||");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::LessThanEq,
                Token::GreaterThanEq,
                Token::NotEqual,
                Token::NotEqual,
                Token::Concat,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_lexer_comments() {
        let mut lexer = Lexer::new("SELECT -- comment\n* FROM events");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Asterisk,
                Token::From,
                Token::Identifier("events".to_string()),
                Token::Eof,
            ]
        );
    }
}
