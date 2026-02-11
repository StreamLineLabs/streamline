//! Shell completion generation and installation helpers
//!
//! Provides dynamic shell completions that can query the running server
//! for topic and group names, plus installation helpers for all major shells.

use clap::CommandFactory;
use clap_complete::{generate, Shell};
use colored::Colorize;
use std::path::PathBuf;

/// Generate shell completions and print to stdout
pub fn generate_completions<C: CommandFactory>(shell: Shell) {
    let mut cmd = C::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut std::io::stdout());
}

/// Print installation instructions for shell completions
pub fn print_installation_instructions(shell: Shell) {
    println!();
    println!("{}", "Shell Completions Installation".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();

    match shell {
        Shell::Bash => print_bash_instructions(),
        Shell::Zsh => print_zsh_instructions(),
        Shell::Fish => print_fish_instructions(),
        Shell::PowerShell => print_powershell_instructions(),
        Shell::Elvish => print_elvish_instructions(),
        _ => {
            println!("  Save the output to a file and source it from your shell config.");
        }
    }
    println!();
}

fn print_bash_instructions() {
    println!(
        "{}",
        "Option 1: User-level installation (recommended)".bold()
    );
    println!();
    println!("  mkdir -p ~/.local/share/bash-completion/completions");
    println!(
        "  {} > ~/.local/share/bash-completion/completions/streamline-cli",
        "streamline-cli completions bash".cyan()
    );
    println!();
    println!("{}", "Option 2: System-wide installation".bold());
    println!();
    println!("  # Linux:");
    println!(
        "  {} | sudo tee /etc/bash_completion.d/streamline-cli > /dev/null",
        "streamline-cli completions bash".cyan()
    );
    println!();
    println!("  # macOS (with Homebrew):");
    println!(
        "  {} > $(brew --prefix)/etc/bash_completion.d/streamline-cli",
        "streamline-cli completions bash".cyan()
    );
    println!();
    println!("{}", "Option 3: Add to .bashrc".bold());
    println!();
    println!("  echo 'eval \"$(streamline-cli completions bash)\"' >> ~/.bashrc");
    println!();
    println!(
        "{}",
        "Then restart your shell or run: source ~/.bashrc".dimmed()
    );
}

fn print_zsh_instructions() {
    println!("{}", "Option 1: Oh My Zsh (recommended)".bold());
    println!();
    println!("  mkdir -p ~/.oh-my-zsh/completions");
    println!(
        "  {} > ~/.oh-my-zsh/completions/_streamline-cli",
        "streamline-cli completions zsh".cyan()
    );
    println!();
    println!("{}", "Option 2: Standard fpath".bold());
    println!();
    println!("  # Add this to ~/.zshrc BEFORE compinit:");
    println!("  fpath=(~/.zsh/completions $fpath)");
    println!();
    println!("  # Then generate completions:");
    println!("  mkdir -p ~/.zsh/completions");
    println!(
        "  {} > ~/.zsh/completions/_streamline-cli",
        "streamline-cli completions zsh".cyan()
    );
    println!();
    println!("{}", "Option 3: Add to .zshrc".bold());
    println!();
    println!("  echo 'eval \"$(streamline-cli completions zsh)\"' >> ~/.zshrc");
    println!();
    println!(
        "{}",
        "Then restart your shell or run: source ~/.zshrc".dimmed()
    );
    println!();
    println!(
        "{}",
        "Note: You may need to run 'rm ~/.zcompdump*' and restart zsh".yellow()
    );
}

fn print_fish_instructions() {
    println!("{}", "Fish completions (auto-discovered)".bold());
    println!();
    println!(
        "  {} > ~/.config/fish/completions/streamline-cli.fish",
        "streamline-cli completions fish".cyan()
    );
    println!();
    println!(
        "{}",
        "Fish will automatically load completions from this directory.".dimmed()
    );
}

fn print_powershell_instructions() {
    println!("{}", "PowerShell completions".bold());
    println!();
    println!("  # Add to your PowerShell profile ($PROFILE):");
    println!("  Invoke-Expression (& streamline-cli completions powershell | Out-String)");
    println!();
    println!("  # Or save to a file and dot-source it:");
    println!(
        "  {} > ~/.config/powershell/streamline-cli.ps1",
        "streamline-cli completions powershell".cyan()
    );
    println!("  # Add to $PROFILE: . ~/.config/powershell/streamline-cli.ps1");
}

fn print_elvish_instructions() {
    println!("{}", "Elvish completions".bold());
    println!();
    println!(
        "  {} > ~/.elvish/lib/streamline-cli.elv",
        "streamline-cli completions elvish".cyan()
    );
    println!();
    println!("  # Add to ~/.elvish/rc.elv:");
    println!("  use streamline-cli");
}

/// Install completions directly to the appropriate location
pub fn install_completions<C: CommandFactory>(shell: Shell) -> Result<PathBuf, String> {
    let mut cmd = C::command();
    let name = cmd.get_name().to_string();

    let (path, needs_sudo) = get_completion_path(shell)?;

    if needs_sudo {
        return Err(format!(
            "System-wide installation requires sudo. Run:\n  \
            streamline-cli completions {} | sudo tee {} > /dev/null",
            shell_name(shell),
            path.display()
        ));
    }

    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create directory: {}", e))?;
    }

    // Generate and write completions
    let mut file =
        std::fs::File::create(&path).map_err(|e| format!("Failed to create file: {}", e))?;

    generate(shell, &mut cmd, name, &mut file);

    Ok(path)
}

fn get_completion_path(shell: Shell) -> Result<(PathBuf, bool), String> {
    match shell {
        Shell::Bash => {
            // Try user-level first
            if let Some(data_dir) = dirs::data_local_dir() {
                let path = data_dir
                    .join("bash-completion")
                    .join("completions")
                    .join("streamline-cli");
                return Ok((path, false));
            }
            // Fall back to system-wide (requires sudo)
            Ok((PathBuf::from("/etc/bash_completion.d/streamline-cli"), true))
        }
        Shell::Zsh => {
            // Try Oh My Zsh first
            if let Some(home) = dirs::home_dir() {
                let omz_path = home.join(".oh-my-zsh/completions/_streamline-cli");
                if omz_path.parent().map(|p| p.exists()).unwrap_or(false) {
                    return Ok((omz_path, false));
                }
                // Standard user path
                let zsh_path = home.join(".zsh/completions/_streamline-cli");
                return Ok((zsh_path, false));
            }
            Err("Could not determine home directory".into())
        }
        Shell::Fish => {
            if let Some(config_dir) = dirs::config_dir() {
                let path = config_dir.join("fish/completions/streamline-cli.fish");
                return Ok((path, false));
            }
            Err("Could not determine config directory".into())
        }
        Shell::PowerShell => {
            if let Some(config_dir) = dirs::config_dir() {
                let path = config_dir.join("powershell/streamline-cli.ps1");
                return Ok((path, false));
            }
            Err("Could not determine config directory".into())
        }
        _ => Err(format!("Unsupported shell: {:?}", shell)),
    }
}

fn shell_name(shell: Shell) -> &'static str {
    match shell {
        Shell::Bash => "bash",
        Shell::Zsh => "zsh",
        Shell::Fish => "fish",
        Shell::PowerShell => "powershell",
        Shell::Elvish => "elvish",
        _ => "unknown",
    }
}

/// Print a dynamic completion script that queries the CLI for topic/group names
///
/// Unlike static completions, these scripts call back to the CLI binary to get
/// real-time lists of topics and groups for completion.
pub fn print_dynamic_completion_script(shell: Shell) {
    match shell {
        Shell::Bash => print_bash_dynamic_script(),
        Shell::Zsh => print_zsh_dynamic_script(),
        Shell::Fish => print_fish_dynamic_script(),
        _ => {
            eprintln!(
                "Dynamic completions are not supported for {:?}. Use static completions instead.",
                shell
            );
        }
    }
}

fn print_bash_dynamic_script() {
    println!(
        r#"# Streamline CLI dynamic completions for Bash
# Queries the CLI for topic and group names at completion time
# Install: streamline-cli completions bash --dynamic >> ~/.bashrc

_streamline_cli_dynamic() {{
    local cur prev words cword
    _init_completion || return

    # Get the command (first non-option word after 'streamline-cli')
    local cmd=""
    local i
    for ((i=1; i < cword; i++)); do
        case "${{words[i]}}" in
            -*) continue ;;
            *) cmd="${{words[i]}}"; break ;;
        esac
    done

    case "$cmd" in
        produce|consume)
            # Complete topic names after produce/consume command
            if [[ $i -eq $((cword - 1)) ]] || [[ "$prev" == "produce" ]] || [[ "$prev" == "consume" ]]; then
                local topics
                topics=$(streamline-cli completions bash --complete-topics 2>/dev/null)
                COMPREPLY=( $(compgen -W "$topics" -- "$cur") )
                return
            fi
            ;;
        topics)
            # After 'topics describe' or 'topics delete', complete topic names
            if [[ "${{words[i+1]}}" == "describe" ]] || [[ "${{words[i+1]}}" == "delete" ]]; then
                local topics
                topics=$(streamline-cli completions bash --complete-topics 2>/dev/null)
                COMPREPLY=( $(compgen -W "$topics" -- "$cur") )
                return
            fi
            ;;
        groups)
            # After 'groups describe', 'groups delete', or 'groups reset', complete group names
            if [[ "${{words[i+1]}}" == "describe" ]] || [[ "${{words[i+1]}}" == "delete" ]] || [[ "${{words[i+1]}}" == "reset" ]]; then
                local groups
                groups=$(streamline-cli completions bash --complete-groups 2>/dev/null)
                COMPREPLY=( $(compgen -W "$groups" -- "$cur") )
                return
            fi
            ;;
    esac

    # Fall back to static completions for other cases
    local commands="produce consume topics groups info top doctor demo quickstart benchmark migrate completions shell history help"
    local subcommands_topics="list create describe delete alter watch export import"
    local subcommands_groups="list describe delete reset"

    case "$cmd" in
        topics)
            COMPREPLY=( $(compgen -W "$subcommands_topics" -- "$cur") )
            ;;
        groups)
            COMPREPLY=( $(compgen -W "$subcommands_groups" -- "$cur") )
            ;;
        "")
            COMPREPLY=( $(compgen -W "$commands" -- "$cur") )
            ;;
    esac
}}

complete -F _streamline_cli_dynamic streamline-cli
"#
    );
}

fn print_zsh_dynamic_script() {
    println!(
        r#"#compdef streamline-cli
# Streamline CLI dynamic completions for Zsh
# Queries the CLI for topic and group names at completion time
# Install: streamline-cli completions zsh --dynamic > ~/.zsh/completions/_streamline-cli

_streamline_cli() {{
    local curcontext="$curcontext" state line
    typeset -A opt_args

    _arguments -C \
        '1: :->command' \
        '*: :->args'

    case $state in
        command)
            local commands=(
                'produce:Produce messages to a topic'
                'consume:Consume messages from a topic'
                'topics:Manage topics'
                'groups:Manage consumer groups'
                'info:Show system information'
                'top:TUI dashboard'
                'doctor:Run diagnostics'
                'demo:One-command demo'
                'quickstart:Interactive setup wizard'
                'benchmark:Run benchmarks'
                'migrate:Migration tools'
                'completions:Generate shell completions'
                'shell:Interactive REPL'
                'history:Show command history'
            )
            _describe 'command' commands
            ;;
        args)
            case $line[1] in
                produce|consume)
                    # Complete topic names
                    local topics
                    topics=(${{(f)"$(streamline-cli completions zsh --complete-topics 2>/dev/null)"}})
                    _describe 'topic' topics
                    ;;
                topics)
                    case $line[2] in
                        describe|delete|alter|watch|export)
                            local topics
                            topics=(${{(f)"$(streamline-cli completions zsh --complete-topics 2>/dev/null)"}})
                            _describe 'topic' topics
                            ;;
                        *)
                            local subcommands=(
                                'list:List all topics'
                                'create:Create a new topic'
                                'describe:Describe a topic'
                                'delete:Delete a topic'
                                'alter:Alter topic configuration'
                                'watch:Watch topic metrics'
                                'export:Export topic data'
                                'import:Import topic data'
                            )
                            _describe 'subcommand' subcommands
                            ;;
                    esac
                    ;;
                groups)
                    case $line[2] in
                        describe|delete|reset)
                            local groups
                            groups=(${{(f)"$(streamline-cli completions zsh --complete-groups 2>/dev/null)"}})
                            _describe 'group' groups
                            ;;
                        *)
                            local subcommands=(
                                'list:List all consumer groups'
                                'describe:Describe a consumer group'
                                'delete:Delete a consumer group'
                                'reset:Reset consumer group offsets'
                            )
                            _describe 'subcommand' subcommands
                            ;;
                    esac
                    ;;
            esac
            ;;
    esac
}}

_streamline_cli "$@"
"#
    );
}

fn print_fish_dynamic_script() {
    println!(
        r#"# Streamline CLI dynamic completions for Fish
# Queries the CLI for topic and group names at completion time
# Install: streamline-cli completions fish --dynamic > ~/.config/fish/completions/streamline-cli.fish

# Disable file completions for streamline-cli
complete -c streamline-cli -f

# Helper function to get topics
function __streamline_topics
    streamline-cli completions fish --complete-topics 2>/dev/null
end

# Helper function to get groups
function __streamline_groups
    streamline-cli completions fish --complete-groups 2>/dev/null
end

# Main commands
complete -c streamline-cli -n "__fish_use_subcommand" -a "produce" -d "Produce messages to a topic"
complete -c streamline-cli -n "__fish_use_subcommand" -a "consume" -d "Consume messages from a topic"
complete -c streamline-cli -n "__fish_use_subcommand" -a "topics" -d "Manage topics"
complete -c streamline-cli -n "__fish_use_subcommand" -a "groups" -d "Manage consumer groups"
complete -c streamline-cli -n "__fish_use_subcommand" -a "info" -d "Show system information"
complete -c streamline-cli -n "__fish_use_subcommand" -a "top" -d "TUI dashboard"
complete -c streamline-cli -n "__fish_use_subcommand" -a "doctor" -d "Run diagnostics"
complete -c streamline-cli -n "__fish_use_subcommand" -a "demo" -d "One-command demo"
complete -c streamline-cli -n "__fish_use_subcommand" -a "quickstart" -d "Interactive setup wizard"
complete -c streamline-cli -n "__fish_use_subcommand" -a "benchmark" -d "Run benchmarks"
complete -c streamline-cli -n "__fish_use_subcommand" -a "migrate" -d "Migration tools"
complete -c streamline-cli -n "__fish_use_subcommand" -a "completions" -d "Generate shell completions"
complete -c streamline-cli -n "__fish_use_subcommand" -a "shell" -d "Interactive REPL"
complete -c streamline-cli -n "__fish_use_subcommand" -a "history" -d "Show command history"

# Topic completions for produce/consume
complete -c streamline-cli -n "__fish_seen_subcommand_from produce consume" -a "(__streamline_topics)" -d "Topic"

# Topics subcommands
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "list" -d "List all topics"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "create" -d "Create a topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "describe" -d "Describe a topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "delete" -d "Delete a topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "alter" -d "Alter topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "watch" -d "Watch topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "export" -d "Export topic"
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and not __fish_seen_subcommand_from list create describe delete alter watch export import" -a "import" -d "Import topic"

# Topic name completions after topics describe/delete/alter/watch/export
complete -c streamline-cli -n "__fish_seen_subcommand_from topics; and __fish_seen_subcommand_from describe delete alter watch export" -a "(__streamline_topics)" -d "Topic"

# Groups subcommands
complete -c streamline-cli -n "__fish_seen_subcommand_from groups; and not __fish_seen_subcommand_from list describe delete reset" -a "list" -d "List all groups"
complete -c streamline-cli -n "__fish_seen_subcommand_from groups; and not __fish_seen_subcommand_from list describe delete reset" -a "describe" -d "Describe a group"
complete -c streamline-cli -n "__fish_seen_subcommand_from groups; and not __fish_seen_subcommand_from list describe delete reset" -a "delete" -d "Delete a group"
complete -c streamline-cli -n "__fish_seen_subcommand_from groups; and not __fish_seen_subcommand_from list describe delete reset" -a "reset" -d "Reset group offsets"

# Group name completions after groups describe/delete/reset
complete -c streamline-cli -n "__fish_seen_subcommand_from groups; and __fish_seen_subcommand_from describe delete reset" -a "(__streamline_groups)" -d "Group"
"#
    );
}

/// Dynamic completions that query the running server
/// This provides smart autocomplete for topic and group names
pub struct DynamicCompleter {
    data_dir: PathBuf,
}

impl DynamicCompleter {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Get list of topic names for completion
    pub fn complete_topics(&self) -> Vec<String> {
        use crate::TopicManager;

        if !self.data_dir.exists() {
            return vec![];
        }

        match TopicManager::new(&self.data_dir) {
            Ok(manager) => manager
                .list_topics()
                .unwrap_or_default()
                .into_iter()
                .map(|t| t.name)
                .collect(),
            Err(_) => vec![],
        }
    }

    /// Get list of consumer group IDs for completion
    pub fn complete_groups(&self) -> Vec<String> {
        use crate::{GroupCoordinator, TopicManager};
        use std::sync::Arc;

        if !self.data_dir.exists() {
            return vec![];
        }

        // Create TopicManager first
        let topic_manager = match TopicManager::new(&self.data_dir) {
            Ok(tm) => Arc::new(tm),
            Err(_) => return vec![],
        };

        match GroupCoordinator::new(&self.data_dir, topic_manager) {
            Ok(coordinator) => coordinator.list_groups().unwrap_or_default(),
            Err(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_name() {
        assert_eq!(shell_name(Shell::Bash), "bash");
        assert_eq!(shell_name(Shell::Zsh), "zsh");
        assert_eq!(shell_name(Shell::Fish), "fish");
    }
}
