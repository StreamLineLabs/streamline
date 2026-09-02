//! `streamline-cli branch` subcommand — admin-plane HTTP wrapper for
//! `/api/v1/branches/*`.
//!
//! Stability: Experimental. Only compiled when the `branches` feature
//! is enabled (because reqwest is gated behind it).

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use streamline::Result;

use crate::cli_http;

#[derive(Subcommand, Debug)]
pub(crate) enum BranchCli {
    /// Create a new branch
    Create {
        /// Topic name (e.g. "orders")
        topic: String,
        /// Branch name (must match [a-z0-9-]+)
        name: String,
        /// Optional parent branch id (`<topic>/<name>`)
        #[arg(long)]
        parent: Option<String>,
    },

    /// List all branches
    Ls,

    /// Get details for a single branch (id is `<topic>/<name>`)
    Get {
        /// Branch id, e.g. orders/exp-a
        id: String,
    },

    /// Append a message to a branch
    Append {
        /// Branch id, e.g. orders/exp-a
        id: String,
        /// Role (e.g. "user", "assistant", "system")
        #[arg(long)]
        role: String,
        /// Message text
        #[arg(long)]
        text: String,
    },

    /// Read all messages on a branch
    Messages {
        /// Branch id
        id: String,
    },

    /// Delete a branch
    Rm {
        /// Branch id
        id: String,
    },
}

#[derive(Debug, Serialize)]
struct CreateBody<'a> {
    topic: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct AppendBody<'a> {
    role: &'a str,
    text: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
struct BranchView {
    id: String,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    created_at_ms: i64,
    #[serde(default)]
    message_count: u64,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    #[serde(default)]
    items: Vec<BranchView>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MessageView {
    role: String,
    text: String,
    #[serde(default)]
    timestamp_ms: i64,
}

#[derive(Debug, Deserialize)]
struct MessagesResponse {
    #[serde(default)]
    messages: Vec<MessageView>,
}

pub(crate) fn handle(cmd: BranchCli) -> Result<()> {
    let base = cli_http::default_url();
    match cmd {
        BranchCli::Create {
            topic,
            name,
            parent,
        } => {
            let body = CreateBody {
                topic: &topic,
                name: &name,
                parent: parent.as_deref(),
            };
            let view: BranchView = cli_http::post_json(&base, "/api/v1/branches", &body)?;
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        BranchCli::Ls => {
            let resp: ListResponse = cli_http::get_json(&base, "/api/v1/branches")?;
            if resp.items.is_empty() {
                println!("(no branches)");
                return Ok(());
            }
            println!("{:<32} {:<32} {:>8}", "ID", "PARENT", "MSGS");
            for b in resp.items {
                println!(
                    "{:<32} {:<32} {:>8}",
                    b.id,
                    b.parent.unwrap_or_else(|| "-".into()),
                    b.message_count
                );
            }
        }
        BranchCli::Get { id } => {
            let path = format!("/api/v1/branches/{id}");
            let view: BranchView = cli_http::get_json(&base, &path)?;
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        BranchCli::Append { id, role, text } => {
            let body = AppendBody {
                role: &role,
                text: &text,
            };
            let path = format!("/api/v1/branches/{id}/messages");
            let _: serde_json::Value = cli_http::post_json(&base, &path, &body)?;
            println!("ok");
        }
        BranchCli::Messages { id } => {
            let path = format!("/api/v1/branches/{id}/messages");
            let resp: MessagesResponse = cli_http::get_json(&base, &path)?;
            for m in resp.messages {
                println!("[{}] {}: {}", m.timestamp_ms, m.role, m.text);
            }
        }
        BranchCli::Rm { id } => {
            let path = format!("/api/v1/branches/{id}");
            cli_http::delete(&base, &path)?;
            println!("deleted {id}");
        }
    }
    Ok(())
}
