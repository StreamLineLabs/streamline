//! `streamline-cli search` — admin-plane HTTP wrapper for
//! `POST /api/v1/topics/:topic/search`.
//!
//! Stability: Experimental. Only compiled with `semantic-topics`.

#![cfg(feature = "semantic-topics")]

use clap::Args;
use serde::{Deserialize, Serialize};
use streamline::Result;

use crate::cli_http;

#[derive(Args, Debug)]
pub(crate) struct SearchCli {
    /// Topic to search
    pub topic: String,
    /// Query string
    pub query: String,
    /// Top-k results
    #[arg(long, default_value_t = 10)]
    pub k: usize,
}

#[derive(Debug, Serialize)]
struct SearchBody<'a> {
    query: &'a str,
    k: usize,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    hits: Vec<Hit>,
    #[serde(default)]
    took_ms: u32,
}

#[derive(Debug, Deserialize)]
struct Hit {
    partition: i32,
    offset: i64,
    score: f32,
    #[serde(default)]
    value: Option<String>,
}

pub(crate) fn handle(cmd: SearchCli) -> Result<()> {
    let base = cli_http::default_url();
    let body = SearchBody {
        query: &cmd.query,
        k: cmd.k,
    };
    let path = format!("/api/v1/topics/{}/search", cmd.topic);
    let resp: SearchResponse = cli_http::post_json(&base, &path, &body)?;
    if resp.hits.is_empty() {
        println!("(no hits) — took {}ms", resp.took_ms);
        return Ok(());
    }
    println!(
        "{:<8} {:<10} {:>10}  {}",
        "PART", "OFFSET", "SCORE", "VALUE"
    );
    for h in &resp.hits {
        println!(
            "{:<8} {:<10} {:>10.4}  {}",
            h.partition,
            h.offset,
            h.score,
            h.value.as_deref().unwrap_or("")
        );
    }
    println!("({} hit(s), {}ms)", resp.hits.len(), resp.took_ms);
    Ok(())
}
