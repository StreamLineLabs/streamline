//! Interactive quickstart wizard for generating example projects
//!
//! Prompts the user for project preferences and scaffolds a complete
//! producer/consumer project in their chosen language.

use colored::Colorize;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

use super::quickstart_templates;

/// Supported SDK languages for project generation.
#[derive(Debug, Clone, Copy)]
enum Language {
    Python,
    TypeScript,
    Java,
    Go,
    Rust,
    CSharp,
}

impl Language {
    fn label(self) -> &'static str {
        match self {
            Language::Python => "Python",
            Language::TypeScript => "TypeScript/Node.js",
            Language::Java => "Java",
            Language::Go => "Go",
            Language::Rust => "Rust",
            Language::CSharp => "C#/.NET",
        }
    }
}

const LANGUAGES: &[Language] = &[
    Language::Python,
    Language::TypeScript,
    Language::Java,
    Language::Go,
    Language::Rust,
    Language::CSharp,
];

fn prompt(question: &str, default: &str) -> String {
    print!("  {} [{}]: ", question, default.cyan());
    io::stdout().flush().ok();
    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_ok() {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return default.to_string();
        }
        return trimmed.to_string();
    }
    default.to_string()
}

fn prompt_language() -> Language {
    println!("  {}", "Select a language:".bold());
    for (i, lang) in LANGUAGES.iter().enumerate() {
        println!("    {} {}", format!("{})", i + 1).cyan(), lang.label());
    }
    print!("  {} [{}]: ", "Choice", "1".cyan());
    io::stdout().flush().ok();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_ok() {
        let trimmed = input.trim();
        if let Ok(n) = trimmed.parse::<usize>() {
            if n >= 1 && n <= LANGUAGES.len() {
                return LANGUAGES[n - 1];
            }
        }
    }
    LANGUAGES[0]
}

/// Run the interactive quickstart project wizard.
///
/// Prompts for project name, language, and topic, then scaffolds a
/// complete example project on disk.
pub fn run_quickstart_wizard() -> crate::Result<()> {
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!(
        "  {} {}",
        "📦".to_string(),
        "Generate a Sample Project".bold().cyan()
    );
    println!();

    let project_name = prompt("Project name", "streamline-demo");
    println!();
    let language = prompt_language();
    println!();
    let topic = prompt("Topic name", "demo-events");
    println!();

    let files = match language {
        Language::Python => quickstart_templates::generate_python_project(&topic),
        Language::TypeScript => quickstart_templates::generate_node_project(&topic),
        Language::Java => quickstart_templates::generate_java_project(&topic),
        Language::Go => quickstart_templates::generate_go_project(&topic),
        Language::Rust => quickstart_templates::generate_rust_project(&topic),
        Language::CSharp => quickstart_templates::generate_dotnet_project(&topic),
    };

    let project_dir = Path::new(&project_name);
    if project_dir.exists() {
        println!(
            "  {} Directory '{}' already exists. Aborting.",
            "✗".red(),
            project_name
        );
        return Ok(());
    }

    fs::create_dir_all(project_dir)?;

    for (filename, content) in &files {
        let file_path = project_dir.join(filename);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&file_path, content)?;
        println!(
            "  {} {}",
            "✓".green(),
            file_path.display().to_string().dimmed()
        );
    }

    println!();
    println!(
        "  {} Project created in '{}'",
        "✓".green().bold(),
        project_name.cyan()
    );
    println!();
    println!("  {}", "Next steps:".bold());

    match language {
        Language::Python => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} python -m venv venv && source venv/bin/activate", "→".cyan());
            println!("    {} pip install -r requirements.txt", "→".cyan());
            println!("    {} python producer.py", "→".cyan());
        }
        Language::TypeScript => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} npm install", "→".cyan());
            println!("    {} npm run producer", "→".cyan());
        }
        Language::Java => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} mvn compile", "→".cyan());
            println!(
                "    {} mvn exec:java -Dexec.mainClass=\"com.example.Producer\"",
                "→".cyan()
            );
        }
        Language::Go => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} go mod tidy", "→".cyan());
            println!("    {} go run producer/main.go", "→".cyan());
        }
        Language::Rust => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} cargo build", "→".cyan());
            println!("    {} cargo run --bin producer", "→".cyan());
        }
        Language::CSharp => {
            println!("    {} cd {}", "→".cyan(), project_name);
            println!("    {} dotnet build", "→".cyan());
            println!("    {} dotnet run", "→".cyan());
        }
    }

    println!();

    Ok(())
}
