use colored::Colorize;
use dialoguer::Confirm;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use crate::{Cli, OutputFormat};
use streamline::{Result, StreamlineError};

/// CLI context passed to all commands
pub(crate) struct CliContext {
    pub(crate) data_dir: PathBuf,
    pub(crate) format: OutputFormat,
    pub(crate) skip_confirm: bool,
    pub(crate) output_file: Option<PathBuf>,
}

impl CliContext {
    pub(crate) fn new(cli: &Cli) -> Self {
        // Color is enabled if: not disabled via flag AND stdout is a terminal AND no output file
        let use_color = !cli.no_color && std::io::stdout().is_terminal() && cli.output.is_none();

        // Control the colored crate's behavior
        if !use_color {
            colored::control::set_override(false);
        }

        Self {
            data_dir: cli.data_dir.clone(),
            format: cli.format,
            skip_confirm: cli.yes,
            output_file: cli.output.clone(),
        }
    }

    /// Write output to stdout or file
    #[allow(dead_code)]
    pub(crate) fn write_output(&self, content: &str) -> Result<()> {
        use std::io::Write;
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
    #[allow(dead_code)]
    pub(crate) fn is_file_output(&self) -> bool {
        self.output_file.is_some()
    }

    /// Create a spinner for long operations
    pub(crate) fn spinner(&self, message: &str) -> Option<ProgressBar> {
        if !std::io::stdout().is_terminal() {
            return None;
        }

        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
                .template("{spinner:.cyan} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        spinner.set_message(message.to_string());
        spinner.enable_steady_tick(Duration::from_millis(100));
        Some(spinner)
    }

    /// Ask for confirmation before destructive operations
    pub(crate) fn confirm(&self, message: &str) -> bool {
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

        Confirm::new()
            .with_prompt(message)
            .default(false)
            .interact()
            .unwrap_or(false)
    }

    /// Print success message
    pub(crate) fn success(&self, message: &str) {
        println!("{} {}", "✓".green(), message);
    }

    /// Print error message
    pub(crate) fn error(&self, message: &str) {
        eprintln!("{} {}", "✗".red(), message);
    }

    /// Print warning message
    pub(crate) fn warn(&self, message: &str) {
        println!("{} {}", "⚠".yellow(), message);
    }

    /// Print info message
    pub(crate) fn info(&self, message: &str) {
        println!("{} {}", "ℹ".blue(), message);
    }
}
