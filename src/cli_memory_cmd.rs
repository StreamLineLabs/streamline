//! `streamline-cli memory {remember,recall}` — admin-plane HTTP wrapper for
//! `/api/v1/memory/*`.
//!
//! Stability: Experimental. Only compiled with `agent-memory`.

#![cfg(feature = "agent-memory")]

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use streamline::Result;

use crate::cli_http;

#[derive(Subcommand, Debug)]
pub(crate) enum MemoryCli {
    /// Write a memory for an agent
    Remember {
        /// Agent identifier
        #[arg(long)]
        agent: String,
        /// Kind: observation | fact | procedure
        #[arg(long, default_value = "fact")]
        kind: String,
        /// Memory content (free text)
        #[arg(long)]
        content: String,
        /// Importance in [0.0, 1.0]; below 0.3 facts skip semantic mirror
        #[arg(long, default_value_t = 0.5)]
        importance: f32,
        /// Required when kind=procedure — names the skill
        #[arg(long)]
        skill: Option<String>,
        /// Tags (repeatable)
        #[arg(long)]
        tag: Vec<String>,
    },

    /// Recall top-k memories matching a query
    Recall {
        /// Agent identifier
        #[arg(long)]
        agent: String,
        /// Query string
        query: String,
        /// Top-k results
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Min semantic hits before episodic fallback
        #[arg(long, default_value_t = 0)]
        min_hits: usize,
    },
}

#[derive(Debug, Serialize)]
struct RememberBody<'a> {
    agent_id: &'a str,
    kind: &'a str,
    content: &'a str,
    importance: f32,
    tags: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    skill: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct WrittenEntry {
    topic: String,
    offset: i64,
}

#[derive(Debug, Deserialize)]
struct RememberResponse {
    #[serde(default)]
    written: Vec<WrittenEntry>,
}

#[derive(Debug, Serialize)]
struct RecallBody<'a> {
    agent_id: &'a str,
    query: &'a str,
    k: usize,
    min_hits: usize,
}

#[derive(Debug, Deserialize)]
struct RecallHit {
    tier: String,
    topic: String,
    offset: i64,
    content: String,
    score: f32,
}

#[derive(Debug, Deserialize)]
struct RecallResponse {
    #[serde(default)]
    hits: Vec<RecallHit>,
}

pub(crate) fn handle(cmd: MemoryCli) -> Result<()> {
    let base = cli_http::default_url();
    match cmd {
        MemoryCli::Remember {
            agent,
            kind,
            content,
            importance,
            skill,
            tag,
        } => {
            let kind_lower = kind.to_lowercase();
            if kind_lower == "procedure" && skill.is_none() {
                return Err(streamline::StreamlineError::Server(
                    "--skill is required when --kind=procedure".into(),
                ));
            }
            let body = RememberBody {
                agent_id: &agent,
                kind: &kind_lower,
                content: &content,
                importance,
                tags: &tag,
                skill: skill.as_deref(),
            };
            let resp: RememberResponse =
                cli_http::post_json(&base, "/api/v1/memory/remember", &body)?;
            println!("wrote {} entry(ies):", resp.written.len());
            for e in resp.written {
                println!("  {}@{}", e.topic, e.offset);
            }
        }
        MemoryCli::Recall {
            agent,
            query,
            k,
            min_hits,
        } => {
            let body = RecallBody {
                agent_id: &agent,
                query: &query,
                k,
                min_hits,
            };
            let resp: RecallResponse =
                cli_http::post_json(&base, "/api/v1/memory/recall", &body)?;
            if resp.hits.is_empty() {
                println!("(no recall)");
                return Ok(());
            }
            for h in &resp.hits {
                println!(
                    "[{}] {}@{} score={:.4} :: {}",
                    h.tier, h.topic, h.offset, h.score, h.content
                );
            }
        }
    }
    Ok(())
}
