//! CLI context and shared utilities
//!
//! This module contains the `CliContext` struct and related utilities that are
//! shared across all CLI commands.

use crate::error::{Result, StreamlineError};
use clap::ValueEnum;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq)]
pub enum OutputFormat {
    /// Human-readable text output
    #[default]
    Text,
    /// JSON output for scripting
    Json,
    /// CSV output for spreadsheets and data tools
    Csv,
    /// TSV output for tab-separated processing
    Tsv,
}

impl OutputFormat {
    /// Returns the delimiter for CSV/TSV formats
    pub fn delimiter(&self) -> Option<&'static str> {
        match self {
            Self::Csv => Some(","),
            Self::Tsv => Some("\t"),
            _ => None,
        }
    }

    /// Check if this is a delimited format (CSV or TSV)
    pub fn is_delimited(&self) -> bool {
        matches!(self, Self::Csv | Self::Tsv)
    }
}

/// Escape a field for CSV output (handles commas, quotes, newlines)
pub fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Escape a field for TSV output (replaces tabs and newlines)
pub fn tsv_escape(field: &str) -> String {
    field
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// CLI context passed to all command handlers
///
/// Contains shared configuration and utilities for:
/// - Output formatting (text/JSON)
/// - File output redirection
/// - User confirmation prompts
/// - Progress indicators
/// - Status messages
pub struct CliContext {
    /// Data directory for topics and consumer groups
    pub data_dir: PathBuf,
    /// Output format (text or JSON)
    pub format: OutputFormat,
    /// Skip confirmation prompts
    pub skip_confirm: bool,
    /// Output file path (if redirecting to file)
    pub output_file: Option<PathBuf>,
}

impl CliContext {
    /// Create a new CLI context from command-line arguments
    ///
    /// # Arguments
    /// * `data_dir` - Path to the data directory
    /// * `format` - Output format
    /// * `no_color` - Whether to disable colored output
    /// * `skip_confirm` - Whether to skip confirmation prompts
    /// * `output_file` - Optional path to redirect output to a file
    pub fn new(
        data_dir: PathBuf,
        format: OutputFormat,
        no_color: bool,
        skip_confirm: bool,
        output_file: Option<PathBuf>,
    ) -> Self {
        // Color is enabled if: not disabled via flag AND stdout is a terminal AND no output file
        let use_color = !no_color && std::io::stdout().is_terminal() && output_file.is_none();

        // Control the colored crate's behavior
        if !use_color {
            colored::control::set_override(false);
        }

        Self {
            data_dir,
            format,
            skip_confirm,
            output_file,
        }
    }

    /// Write output to stdout or file
    pub fn write_output(&self, content: &str) -> Result<()> {
        match &self.output_file {
            Some(path) => {
                let mut file = std::fs::File::create(path).map_err(StreamlineError::Io)?;
                file.write_all(content.as_bytes())
                    .map_err(StreamlineError::Io)?;
                eprintln!("{} Output written to {}", "✓".green(), path.display());
                Ok(())
            }
            None => {
                print!("{}", content);
                Ok(())
            }
        }
    }

    /// Check if output is going to a file
    pub fn is_file_output(&self) -> bool {
        self.output_file.is_some()
    }

    /// Ask for confirmation before destructive operations
    ///
    /// Returns true if the user confirms, false otherwise.
    /// Automatically returns true if `skip_confirm` is set.
    pub fn confirm(&self, message: &str) -> bool {
        if self.skip_confirm {
            return true;
        }

        if !std::io::stdin().is_terminal() {
            // Non-interactive mode without --yes flag
            eprintln!(
                "{}: Use --yes flag to skip confirmation in non-interactive mode",
                "warning".yellow()
            );
            return false;
        }

        dialoguer::Confirm::new()
            .with_prompt(message)
            .default(false)
            .interact()
            .unwrap_or(false)
    }

    /// Print success message
    pub fn success(&self, message: &str) {
        println!("{} {}", "✓".green(), message);
    }

    /// Print error message to stderr
    pub fn error(&self, message: &str) {
        eprintln!("{} {}", "✗".red(), message);
    }

    /// Print warning message
    pub fn warn(&self, message: &str) {
        println!("{} {}", "⚠".yellow(), message);
    }

    /// Print info message
    pub fn info(&self, message: &str) {
        println!("{} {}", "ℹ".blue(), message);
    }

    /// Print a section header
    pub fn header(&self, title: &str) {
        println!("{}", title.bold().cyan());
        println!("{}", "═".repeat(60).dimmed());
    }

    /// Check if JSON output is requested
    pub fn is_json(&self) -> bool {
        self.format == OutputFormat::Json
    }

    /// Check if CSV output is requested
    pub fn is_csv(&self) -> bool {
        self.format == OutputFormat::Csv
    }

    /// Check if TSV output is requested
    pub fn is_tsv(&self) -> bool {
        self.format == OutputFormat::Tsv
    }

    /// Check if output is a delimited format (CSV or TSV)
    pub fn is_delimited(&self) -> bool {
        self.format.is_delimited()
    }

    /// Format a row for delimited output (CSV or TSV)
    pub fn format_delimited_row(&self, fields: &[&str]) -> String {
        let escaped: Vec<String> = match self.format {
            OutputFormat::Csv => fields.iter().map(|f| csv_escape(f)).collect(),
            OutputFormat::Tsv => fields.iter().map(|f| tsv_escape(f)).collect(),
            _ => fields.iter().map(|s| s.to_string()).collect(),
        };
        let delimiter = self.format.delimiter().unwrap_or(",");
        escaped.join(delimiter)
    }

    /// Print a message (plain output, no formatting)
    pub fn println(&self, message: &str) {
        println!("{}", message);
    }

    /// Create a spinner that auto-finishes or returns a dummy for non-interactive mode
    pub fn spinner(&self, message: &str) -> SpinnerHandle {
        if !std::io::stdout().is_terminal() || self.output_file.is_some() {
            println!("{}...", message);
            return SpinnerHandle(None);
        }

        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner:.green} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        spinner.set_message(message.to_string());
        spinner.enable_steady_tick(Duration::from_millis(100));
        SpinnerHandle(Some(spinner))
    }
}

/// Handle for a spinner that may or may not be active
pub struct SpinnerHandle(Option<ProgressBar>);

impl SpinnerHandle {
    /// Complete the spinner with a message
    pub fn finish_with_message(&self, msg: &str) {
        if let Some(ref pb) = self.0 {
            pb.finish_with_message(msg.to_string());
        }
    }

    /// Complete the spinner successfully
    pub fn finish(&self) {
        if let Some(ref pb) = self.0 {
            pb.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_format_default() {
        assert_eq!(OutputFormat::default(), OutputFormat::Text);
    }

    #[test]
    fn test_cli_context_is_json() {
        let ctx = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Json,
            skip_confirm: false,
            output_file: None,
        };
        assert!(ctx.is_json());

        let ctx = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Text,
            skip_confirm: false,
            output_file: None,
        };
        assert!(!ctx.is_json());
    }

    #[test]
    fn test_cli_context_is_file_output() {
        let ctx = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Text,
            skip_confirm: false,
            output_file: None,
        };
        assert!(!ctx.is_file_output());

        let ctx = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Text,
            skip_confirm: false,
            output_file: Some(PathBuf::from("output.txt")),
        };
        assert!(ctx.is_file_output());
    }

    #[test]
    fn test_confirm_with_skip() {
        let ctx = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Text,
            skip_confirm: true,
            output_file: None,
        };
        assert!(ctx.confirm("Delete everything?"));
    }

    #[test]
    fn test_csv_escape_simple() {
        assert_eq!(csv_escape("hello"), "hello");
        assert_eq!(csv_escape("hello world"), "hello world");
    }

    #[test]
    fn test_csv_escape_comma() {
        assert_eq!(csv_escape("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_csv_escape_quotes() {
        assert_eq!(csv_escape("say \"hello\""), "\"say \"\"hello\"\"\"");
    }

    #[test]
    fn test_csv_escape_newline() {
        assert_eq!(csv_escape("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn test_tsv_escape_simple() {
        assert_eq!(tsv_escape("hello"), "hello");
        assert_eq!(tsv_escape("hello,world"), "hello,world");
    }

    #[test]
    fn test_tsv_escape_tab() {
        assert_eq!(tsv_escape("hello\tworld"), "hello\\tworld");
    }

    #[test]
    fn test_tsv_escape_newline() {
        assert_eq!(tsv_escape("line1\nline2"), "line1\\nline2");
    }

    #[test]
    fn test_output_format_delimiter() {
        assert_eq!(OutputFormat::Csv.delimiter(), Some(","));
        assert_eq!(OutputFormat::Tsv.delimiter(), Some("\t"));
        assert_eq!(OutputFormat::Text.delimiter(), None);
        assert_eq!(OutputFormat::Json.delimiter(), None);
    }

    #[test]
    fn test_output_format_is_delimited() {
        assert!(OutputFormat::Csv.is_delimited());
        assert!(OutputFormat::Tsv.is_delimited());
        assert!(!OutputFormat::Text.is_delimited());
        assert!(!OutputFormat::Json.is_delimited());
    }

    #[test]
    fn test_format_delimited_row() {
        let ctx_csv = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Csv,
            skip_confirm: false,
            output_file: None,
        };
        assert_eq!(
            ctx_csv.format_delimited_row(&["name", "value", "count"]),
            "name,value,count"
        );
        assert_eq!(
            ctx_csv.format_delimited_row(&["hello,world", "test"]),
            "\"hello,world\",test"
        );

        let ctx_tsv = CliContext {
            data_dir: PathBuf::from("./data"),
            format: OutputFormat::Tsv,
            skip_confirm: false,
            output_file: None,
        };
        assert_eq!(
            ctx_tsv.format_delimited_row(&["name", "value", "count"]),
            "name\tvalue\tcount"
        );
    }
}
