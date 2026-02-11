//! Doctor, quickstart, demo, and dev-checks CLI commands

use serde_json::json;
use streamline::Result;

use crate::cli_context::CliContext;
use crate::OutputFormat;

pub(super) fn handle_doctor_command(
    server_addr: &str,
    http_addr: &str,
    deep: bool,
    dev: bool,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::cli_utils::doctor::{
        print_diagnostic_summary, run_diagnostics, DiagnosticConfig,
    };

    // If --dev flag is set, run developer environment checks
    if dev {
        let results = run_dev_environment_checks();
        match ctx.format {
            OutputFormat::Json => {
                let json_results: Vec<serde_json::Value> = results
                    .iter()
                    .map(|r| {
                        json!({
                            "name": r.name,
                            "status": format!("{:?}", r.status),
                            "message": r.message,
                            "fix": r.fix
                        })
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json_results)?
                );
            }
            _ => {
                print_dev_diagnostic_summary(&results);
            }
        }
        return Ok(());
    }

    let config = DiagnosticConfig {
        data_dir: ctx.data_dir.clone(),
        server_addr: server_addr.to_string(),
        http_addr: http_addr.to_string(),
        verbose: false,
        deep,
    };

    let results = run_diagnostics(&config);

    match ctx.format {
        OutputFormat::Json => {
            let json_results: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    json!({
                        "name": r.name,
                        "status": format!("{:?}", r.status),
                        "message": r.message,
                        "fix": r.fix
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&json_results)?
            );
        }
        _ => {
            print_diagnostic_summary(&results);
        }
    }

    Ok(())
}

/// Check result for dev environment diagnostics
struct DevCheckResult {
    name: String,
    status: &'static str,
    message: String,
    fix: Option<String>,
}

impl DevCheckResult {
    fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "Pass",
            message: message.into(),
            fix: None,
        }
    }

    fn fail(name: impl Into<String>, message: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "Fail",
            message: message.into(),
            fix: Some(fix.into()),
        }
    }

    fn warn(name: impl Into<String>, message: impl Into<String>, fix: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "Warn",
            message: message.into(),
            fix: Some(fix.into()),
        }
    }
}

/// Run development environment checks
fn run_dev_environment_checks() -> Vec<DevCheckResult> {
    use std::process::Command;
    let mut results = Vec::new();

    // Check Rust version
    if let Ok(output) = Command::new("rustc").arg("--version").output() {
        let version = String::from_utf8_lossy(&output.stdout);
        if version.contains("1.75")
            || version.contains("1.76")
            || version.contains("1.77")
            || version.contains("1.78")
            || version.contains("1.79")
            || version.contains("1.80")
            || version.contains("1.8")
        {
            results.push(DevCheckResult::pass(
                "Rust Version",
                format!("Installed: {}", version.trim()),
            ));
        } else {
            results.push(DevCheckResult::warn(
                "Rust Version",
                format!("Installed: {} (1.75+ recommended)", version.trim()),
                "rustup update stable",
            ));
        }
    } else {
        results.push(DevCheckResult::fail(
            "Rust Version",
            "Rust not found",
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
        ));
    }

    // Check rustfmt
    if Command::new("rustfmt").arg("--version").output().is_ok() {
        results.push(DevCheckResult::pass("rustfmt", "Installed"));
    } else {
        results.push(DevCheckResult::fail(
            "rustfmt",
            "Not installed",
            "rustup component add rustfmt",
        ));
    }

    // Check clippy
    if Command::new("cargo")
        .args(["clippy", "--version"])
        .output()
        .is_ok()
    {
        results.push(DevCheckResult::pass("clippy", "Installed"));
    } else {
        results.push(DevCheckResult::fail(
            "clippy",
            "Not installed",
            "rustup component add clippy",
        ));
    }

    // Check cargo-watch
    if Command::new("cargo")
        .args(["watch", "--version"])
        .output()
        .is_ok()
    {
        results.push(DevCheckResult::pass("cargo-watch", "Installed"));
    } else {
        results.push(DevCheckResult::warn(
            "cargo-watch",
            "Not installed (optional)",
            "cargo install cargo-watch",
        ));
    }

    // Check cargo-audit
    if Command::new("cargo")
        .args(["audit", "--version"])
        .output()
        .is_ok()
    {
        results.push(DevCheckResult::pass("cargo-audit", "Installed"));
    } else {
        results.push(DevCheckResult::warn(
            "cargo-audit",
            "Not installed (optional)",
            "cargo install cargo-audit",
        ));
    }

    // Check cargo-deny
    if Command::new("cargo")
        .args(["deny", "--version"])
        .output()
        .is_ok()
    {
        results.push(DevCheckResult::pass("cargo-deny", "Installed"));
    } else {
        results.push(DevCheckResult::warn(
            "cargo-deny",
            "Not installed (optional)",
            "cargo install cargo-deny",
        ));
    }

    // Check cargo-nextest
    if Command::new("cargo")
        .args(["nextest", "--version"])
        .output()
        .is_ok()
    {
        results.push(DevCheckResult::pass("cargo-nextest", "Installed"));
    } else {
        results.push(DevCheckResult::warn(
            "cargo-nextest",
            "Not installed (optional - faster test runner)",
            "cargo install cargo-nextest",
        ));
    }

    // Check pre-commit
    if Command::new("pre-commit").arg("--version").output().is_ok() {
        results.push(DevCheckResult::pass("pre-commit", "Installed"));

        // Check if hooks are installed
        if std::path::Path::new(".git/hooks/pre-commit").exists() {
            results.push(DevCheckResult::pass("pre-commit hooks", "Installed"));
        } else {
            results.push(DevCheckResult::warn(
                "pre-commit hooks",
                "Not installed",
                "pre-commit install",
            ));
        }
    } else {
        results.push(DevCheckResult::warn(
            "pre-commit",
            "Not installed (optional)",
            "pip install pre-commit && pre-commit install",
        ));
    }

    // Check git
    if Command::new("git").arg("--version").output().is_ok() {
        results.push(DevCheckResult::pass("git", "Installed"));
    } else {
        results.push(DevCheckResult::fail(
            "git",
            "Not installed",
            "Install git from https://git-scm.com/",
        ));
    }

    // Check if we're in a git repo
    if std::path::Path::new(".git").exists() {
        results.push(DevCheckResult::pass("Git repository", "Initialized"));
    } else {
        results.push(DevCheckResult::fail(
            "Git repository",
            "Not a git repository",
            "git init",
        ));
    }

    // Check rust-toolchain.toml
    if std::path::Path::new("rust-toolchain.toml").exists() {
        results.push(DevCheckResult::pass("rust-toolchain.toml", "Present"));
    } else {
        results.push(DevCheckResult::warn(
            "rust-toolchain.toml",
            "Not present",
            "Create rust-toolchain.toml to pin Rust version",
        ));
    }

    // Check .cargo/config.toml
    if std::path::Path::new(".cargo/config.toml").exists() {
        results.push(DevCheckResult::pass(
            "Cargo config",
            ".cargo/config.toml present",
        ));
    } else {
        results.push(DevCheckResult::warn(
            "Cargo config",
            ".cargo/config.toml not present",
            "Run ./scripts/dev-setup.sh to create",
        ));
    }

    // Check VS Code workspace
    if std::path::Path::new(".vscode").exists() {
        results.push(DevCheckResult::pass(
            "VS Code workspace",
            ".vscode/ present",
        ));
    } else {
        results.push(DevCheckResult::warn(
            "VS Code workspace",
            ".vscode/ not present",
            "Run ./scripts/dev-setup.sh to create",
        ));
    }

    // Check if project builds
    results.push(DevCheckResult::pass(
        "Build check",
        "Run 'cargo check' to verify build",
    ));

    results
}

/// Print dev environment diagnostic summary
fn print_dev_diagnostic_summary(results: &[DevCheckResult]) {
    use colored::Colorize;

    println!();
    println!("{}", "Development Environment Check".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();

    let mut pass_count = 0;
    let mut fail_count = 0;
    let mut warn_count = 0;

    for result in results {
        let icon = match result.status {
            "Pass" => "✓".green(),
            "Fail" => "✗".red(),
            "Warn" => "⚠".yellow(),
            _ => "○".dimmed(),
        };

        println!("{} {}", icon, result.name);
        println!("  {}", result.message.dimmed());
        if let Some(fix) = &result.fix {
            println!("  {} {}", "→".cyan(), fix);
        }
        println!();

        match result.status {
            "Pass" => pass_count += 1,
            "Fail" => fail_count += 1,
            "Warn" => warn_count += 1,
            _ => {}
        }
    }

    println!("{}", "─".repeat(60).dimmed());
    println!(
        "  {} passed, {} failed, {} warnings",
        pass_count.to_string().green(),
        fail_count.to_string().red(),
        warn_count.to_string().yellow(),
    );

    if fail_count > 0 {
        println!();
        println!(
            "  {} Run the suggested fixes above to resolve issues.",
            "→".cyan()
        );
    } else if warn_count > 0 {
        println!();
        println!(
            "  {} Optional tools can improve your development experience.",
            "ℹ".blue()
        );
    } else {
        println!();
        println!("  {} Development environment is fully set up!", "✓".green());
    }
    println!();
}

/// Handle the quickstart command - interactive wizard
pub(super) fn handle_quickstart_command(
    interactive: bool,
    with_sample_data: bool,
    start_server: bool,
    server_addr: String,
    http_addr: String,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::cli_utils::quickstart::{run_quickstart, QuickstartConfig};

    let config = QuickstartConfig {
        data_dir: ctx.data_dir.clone(),
        with_sample_data,
        interactive,
        start_server,
        server_addr,
        http_addr,
    };

    run_quickstart(&config)?;
    Ok(())
}

pub(super) fn handle_demo_command(
    with_sample_data: bool,
    server_addr: String,
    http_addr: String,
    ctx: &CliContext,
) -> Result<()> {
    handle_quickstart_command(false, with_sample_data, true, server_addr, http_addr, ctx)
}
