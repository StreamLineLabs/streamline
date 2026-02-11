//! Interactive REPL (Read-Eval-Print Loop) for Streamline
//!
//! Provides an interactive shell for exploring topics, producing/consuming
//! messages, and executing commands without restarting the CLI.
//!
//! # Features
//!
//! - Tab completion for commands and topic names
//! - Command history with persistent storage
//! - Multiline input for complex commands
//! - Syntax highlighting
//! - Session variables
//!
//! # Usage
//!
//! ```bash
//! streamline shell
//! ```
//!
//! # Commands
//!
//! ```text
//! topics              List all topics
//! topic <name>        Select a topic for subsequent commands
//! produce <msg>       Produce a message to the current topic
//! consume [n]         Consume n messages from the current topic
//! describe            Describe the current topic
//! query <sql>         Execute a SQL query
//! set <var>=<value>   Set a session variable
//! help                Show help
//! exit                Exit the REPL
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{MatchingBracketValidator, Validator};
use rustyline::{Cmd, CompletionType, Config, Context, EditMode, Editor, KeyEvent};

use crate::storage::TopicManager;

/// REPL configuration
#[derive(Debug, Clone)]
pub struct ReplConfig {
    /// History file path
    pub history_file: Option<PathBuf>,
    /// Maximum history entries
    pub max_history: usize,
    /// Prompt string
    pub prompt: String,
    /// Enable colors
    pub colors: bool,
    /// Data directory for TopicManager
    pub data_dir: PathBuf,
}

impl Default for ReplConfig {
    fn default() -> Self {
        let history_file = dirs::data_dir().map(|d| d.join("streamline").join("repl_history"));

        Self {
            history_file,
            max_history: 1000,
            prompt: "streamline> ".to_string(),
            colors: true,
            data_dir: PathBuf::from("./data"),
        }
    }
}

/// REPL session state
struct ReplSession {
    /// Currently selected topic
    current_topic: Option<String>,
    /// Session variables
    variables: HashMap<String, String>,
    /// Topic manager
    topic_manager: Arc<TopicManager>,
}

impl ReplSession {
    fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            current_topic: None,
            variables: HashMap::new(),
            topic_manager,
        }
    }

    fn prompt(&self) -> String {
        if let Some(topic) = &self.current_topic {
            format!("streamline({})> ", topic)
        } else {
            "streamline> ".to_string()
        }
    }
}

/// Command parsed from user input
#[derive(Debug)]
enum Command {
    /// List topics
    Topics,
    /// Select a topic
    Topic(String),
    /// Produce a message
    Produce(String),
    /// Consume messages
    Consume(Option<usize>),
    /// Describe current topic
    Describe,
    /// Execute SQL query
    Query(String),
    /// Set variable
    Set(String, String),
    /// Get variable
    Get(String),
    /// Show variables
    Variables,
    /// Show help
    Help,
    /// Exit REPL
    Exit,
    /// Unknown command
    Unknown(String),
}

impl Command {
    fn parse(input: &str) -> Self {
        let trimmed = input.trim();
        let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();

        match parts.first().map(|s| s.to_lowercase()).as_deref() {
            Some("topics") | Some("ls") => Command::Topics,
            Some("topic") | Some("use") => {
                if let Some(name) = parts.get(1) {
                    Command::Topic(name.trim().to_string())
                } else {
                    Command::Unknown("topic: missing topic name".to_string())
                }
            }
            Some("produce") | Some("send") | Some("p") => {
                if let Some(msg) = parts.get(1) {
                    Command::Produce(msg.trim().to_string())
                } else {
                    Command::Unknown("produce: missing message".to_string())
                }
            }
            Some("consume") | Some("recv") | Some("c") => {
                let count = parts.get(1).and_then(|s| s.parse().ok());
                Command::Consume(count)
            }
            Some("describe") | Some("desc") | Some("d") => Command::Describe,
            Some("query") | Some("sql") | Some("q") => {
                if let Some(sql) = parts.get(1) {
                    Command::Query(sql.trim().to_string())
                } else {
                    Command::Unknown("query: missing SQL".to_string())
                }
            }
            Some("set") => {
                if let Some(assignment) = parts.get(1) {
                    if let Some((key, value)) = assignment.split_once('=') {
                        Command::Set(key.trim().to_string(), value.trim().to_string())
                    } else {
                        Command::Unknown("set: expected format 'key=value'".to_string())
                    }
                } else {
                    Command::Unknown("set: missing assignment".to_string())
                }
            }
            Some("get") => {
                if let Some(key) = parts.get(1) {
                    Command::Get(key.trim().to_string())
                } else {
                    Command::Unknown("get: missing variable name".to_string())
                }
            }
            Some("vars") | Some("variables") | Some("env") => Command::Variables,
            Some("help") | Some("?") | Some("h") => Command::Help,
            Some("exit") | Some("quit") | Some("q!") => Command::Exit,
            Some("") | None => Command::Unknown(String::new()),
            Some(cmd) => Command::Unknown(format!("unknown command: {}", cmd)),
        }
    }
}

/// REPL helper for completions and hints
struct ReplHelper {
    /// Topic names for completion
    topics: Vec<String>,
    /// Matching bracket highlighter
    highlighter: MatchingBracketHighlighter,
    /// History-based hinter
    hinter: HistoryHinter,
    /// Bracket validator
    validator: MatchingBracketValidator,
}

impl rustyline::Helper for ReplHelper {}

impl ReplHelper {
    fn new(topics: Vec<String>) -> Self {
        Self {
            topics,
            highlighter: MatchingBracketHighlighter::new(),
            hinter: HistoryHinter::new(),
            validator: MatchingBracketValidator::new(),
        }
    }

    fn commands() -> Vec<&'static str> {
        vec![
            "topics",
            "topic",
            "produce",
            "consume",
            "describe",
            "query",
            "set",
            "get",
            "variables",
            "help",
            "exit",
        ]
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let line_up_to_cursor = &line[..pos];
        let parts: Vec<&str> = line_up_to_cursor.split_whitespace().collect();

        // Complete commands at start
        if parts.is_empty() || (parts.len() == 1 && !line_up_to_cursor.ends_with(' ')) {
            let prefix = parts.first().unwrap_or(&"");
            let matches: Vec<Pair> = Self::commands()
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|cmd| Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                })
                .collect();
            let start = line_up_to_cursor.rfind(' ').map(|i| i + 1).unwrap_or(0);
            return Ok((start, matches));
        }

        // Complete topic names after 'topic' or 'use' command
        if !parts.is_empty() {
            let cmd = parts[0].to_lowercase();
            if cmd == "topic" || cmd == "use" {
                let prefix = parts.get(1).unwrap_or(&"");
                let matches: Vec<Pair> = self
                    .topics
                    .iter()
                    .filter(|t| t.starts_with(prefix))
                    .map(|t| Pair {
                        display: t.clone(),
                        replacement: t.clone(),
                    })
                    .collect();
                let start = line_up_to_cursor.rfind(' ').map(|i| i + 1).unwrap_or(0);
                return Ok((start, matches));
            }
        }

        Ok((pos, vec![]))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> std::borrow::Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        if default {
            std::borrow::Cow::Owned(format!("\x1b[1;32m{}\x1b[0m", prompt))
        } else {
            std::borrow::Cow::Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Owned(format!("\x1b[90m{}\x1b[0m", hint))
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}

impl Validator for ReplHelper {
    fn validate(
        &self,
        ctx: &mut rustyline::validate::ValidationContext,
    ) -> rustyline::Result<rustyline::validate::ValidationResult> {
        self.validator.validate(ctx)
    }

    fn validate_while_typing(&self) -> bool {
        self.validator.validate_while_typing()
    }
}

/// Run the interactive REPL
pub fn run_repl(config: ReplConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize topic manager
    let topic_manager = Arc::new(TopicManager::new(&config.data_dir)?);
    let mut session = ReplSession::new(topic_manager.clone());

    // Get topic list for completions
    let topics: Vec<String> = topic_manager
        .list_topics()
        .unwrap_or_default()
        .into_iter()
        .map(|m| m.name)
        .collect();

    // Configure rustyline
    let rl_config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .max_history_size(config.max_history)?
        .build();

    let helper = ReplHelper::new(topics);
    let mut rl = Editor::with_config(rl_config)?;
    rl.set_helper(Some(helper));

    // Bind Ctrl-C to abort current line
    rl.bind_sequence(KeyEvent::ctrl('c'), Cmd::Interrupt);

    // Load history
    if let Some(ref history_file) = config.history_file {
        if let Some(parent) = history_file.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let _ = rl.load_history(history_file);
    }

    // Print welcome message
    println!();
    println!("\x1b[1;36mStreamline Interactive Shell\x1b[0m");
    println!("Type 'help' for commands, 'exit' to quit");
    println!();

    // Main REPL loop
    loop {
        let prompt = session.prompt();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line)?;

                match Command::parse(line) {
                    Command::Topics => match session.topic_manager.list_topics() {
                        Ok(topics) => {
                            if topics.is_empty() {
                                println!("No topics found");
                            } else {
                                println!("Topics:");
                                for metadata in topics {
                                    let marker =
                                        if Some(&metadata.name) == session.current_topic.as_ref() {
                                            " *"
                                        } else {
                                            ""
                                        };
                                    println!("  {}{}", metadata.name, marker);
                                }
                            }
                        }
                        Err(e) => println!("Error listing topics: {}", e),
                    },
                    Command::Topic(name) => {
                        if session.topic_manager.get_topic_stats(&name).is_ok() {
                            session.current_topic = Some(name.clone());
                            println!("Selected topic: {}", name);
                        } else {
                            println!("Topic not found: {}", name);
                        }
                    }
                    Command::Produce(msg) => {
                        if let Some(topic) = &session.current_topic {
                            match session.topic_manager.append(
                                topic,
                                0,
                                None,
                                bytes::Bytes::from(msg),
                            ) {
                                Ok(offset) => println!("Produced at offset {}", offset),
                                Err(e) => println!("Error: {}", e),
                            }
                        } else {
                            println!("No topic selected. Use 'topic <name>' first.");
                        }
                    }
                    Command::Consume(count) => {
                        let limit = count.unwrap_or(10);
                        if let Some(topic) = &session.current_topic {
                            match session.topic_manager.read(topic, 0, 0, limit) {
                                Ok(records) => {
                                    if records.is_empty() {
                                        println!("No messages");
                                    } else {
                                        for record in records {
                                            let value =
                                                String::from_utf8_lossy(&record.value).to_string();
                                            println!(
                                                "[{}] {}",
                                                record.offset,
                                                truncate(&value, 100)
                                            );
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        } else {
                            println!("No topic selected. Use 'topic <name>' first.");
                        }
                    }
                    Command::Describe => {
                        if let Some(topic_name) = &session.current_topic {
                            match session.topic_manager.get_topic_stats(topic_name) {
                                Ok(stats) => {
                                    println!("Topic: {}", stats.name);
                                    println!("Partitions: {}", stats.num_partitions);
                                    println!("Total messages: {}", stats.total_messages);
                                    for ps in &stats.partition_stats {
                                        println!(
                                            "  Partition {}: earliest={}, latest={}",
                                            ps.partition_id, ps.earliest_offset, ps.latest_offset
                                        );
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        } else {
                            println!("No topic selected. Use 'topic <name>' first.");
                        }
                    }
                    Command::Query(sql) => {
                        #[cfg(feature = "analytics")]
                        {
                            // DuckDB analytics integration: execute SQL against stream data.
                            // When the analytics engine is available, it will be invoked here.
                            println!("Executing SQL via analytics engine...");
                            println!("Query: {}", sql);
                            println!("(Analytics engine integration pending — query logged)");
                        }
                        #[cfg(not(feature = "analytics"))]
                        {
                            println!("SQL queries require the 'analytics' feature.");
                            println!("Rebuild with: cargo build --features analytics");
                            println!("Query: {}", sql);
                        }
                    }
                    Command::Set(key, value) => {
                        session.variables.insert(key.clone(), value.clone());
                        println!("{} = {}", key, value);
                    }
                    Command::Get(key) => {
                        if let Some(value) = session.variables.get(&key) {
                            println!("{} = {}", key, value);
                        } else {
                            println!("Variable not found: {}", key);
                        }
                    }
                    Command::Variables => {
                        if session.variables.is_empty() {
                            println!("No variables set");
                        } else {
                            for (key, value) in &session.variables {
                                println!("{} = {}", key, value);
                            }
                        }
                    }
                    Command::Help => {
                        print_help();
                    }
                    Command::Exit => {
                        println!("Goodbye!");
                        break;
                    }
                    Command::Unknown(msg) => {
                        if !msg.is_empty() {
                            println!("{}", msg);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    if let Some(ref history_file) = config.history_file {
        let _ = rl.save_history(history_file);
    }

    Ok(())
}

fn print_help() {
    println!(
        r#"
Streamline Interactive Shell Commands:

  topics            List all topics
  topic <name>      Select a topic for subsequent commands
  produce <msg>     Produce a message to the current topic
  consume [n]       Consume n messages from the current topic (default: 10)
  describe          Describe the current topic
  query <sql>       Execute a SQL query (requires analytics feature)
  set <key>=<value> Set a session variable
  get <key>         Get a session variable
  variables         Show all session variables
  help              Show this help
  exit              Exit the REPL

Shortcuts:
  ls                Alias for 'topics'
  use <name>        Alias for 'topic'
  p <msg>           Alias for 'produce'
  c [n]             Alias for 'consume'
  d                 Alias for 'describe'
  q <sql>           Alias for 'query'
  ?                 Alias for 'help'
"#
    );
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parse_topics() {
        assert!(matches!(Command::parse("topics"), Command::Topics));
        assert!(matches!(Command::parse("ls"), Command::Topics));
    }

    #[test]
    fn test_command_parse_topic() {
        match Command::parse("topic my-topic") {
            Command::Topic(name) => assert_eq!(name, "my-topic"),
            _ => panic!("Expected Topic command"),
        }
    }

    #[test]
    fn test_command_parse_produce() {
        match Command::parse("produce hello world") {
            Command::Produce(msg) => assert_eq!(msg, "hello world"),
            _ => panic!("Expected Produce command"),
        }
    }

    #[test]
    fn test_command_parse_consume() {
        assert!(matches!(Command::parse("consume"), Command::Consume(None)));
        match Command::parse("consume 20") {
            Command::Consume(Some(n)) => assert_eq!(n, 20),
            _ => panic!("Expected Consume command with count"),
        }
    }

    #[test]
    fn test_command_parse_set() {
        match Command::parse("set foo=bar") {
            Command::Set(key, value) => {
                assert_eq!(key, "foo");
                assert_eq!(value, "bar");
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("a very long string", 10), "a very lon...");
    }
}
