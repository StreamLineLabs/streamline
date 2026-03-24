//! Tiny blocking HTTP helper for admin CLI subcommands.
//!
//! Used by `streamline-cli branch|contract|attest`. We deliberately use
//! reqwest's blocking client so the existing synchronous CLI structure
//! doesn't need a tokio runtime per command.
//!
//! Available only when the corresponding moonshot feature is enabled
//! (`branches` or `attestation`), since reqwest is gated on those.

#![cfg(any(
    feature = "branches",
    feature = "attestation",
    feature = "semantic-topics",
    feature = "agent-memory"
))]

use serde::de::DeserializeOwned;
use std::time::Duration;
use streamline::Result;
use streamline::StreamlineError;

const DEFAULT_TIMEOUT_SECS: u64 = 10;

fn client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .build()
        .map_err(|e| StreamlineError::Server(format!("http client init: {e}")))
}

pub fn default_url() -> String {
    std::env::var("STREAMLINE_HTTP_URL")
        .unwrap_or_else(|_| "http://localhost:9094".to_string())
}

pub fn get_json<T: DeserializeOwned>(base: &str, path: &str) -> Result<T> {
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let resp = client()?
        .get(&url)
        .send()
        .map_err(|e| StreamlineError::Server(format!("GET {url}: {e}")))?;
    parse(resp)
}

pub fn post_json<B: serde::Serialize, T: DeserializeOwned>(
    base: &str,
    path: &str,
    body: &B,
) -> Result<T> {
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let resp = client()?
        .post(&url)
        .json(body)
        .send()
        .map_err(|e| StreamlineError::Server(format!("POST {url}: {e}")))?;
    parse(resp)
}

/// POST returning the raw status + parsed JSON, so callers that treat 4xx as
/// a *valid* outcome (e.g. contract validation rejection) can branch.
#[allow(dead_code)]
pub fn post_json_with_status<B: serde::Serialize>(
    base: &str,
    path: &str,
    body: &B,
) -> Result<(u16, serde_json::Value)> {
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let resp = client()?
        .post(&url)
        .json(body)
        .send()
        .map_err(|e| StreamlineError::Server(format!("POST {url}: {e}")))?;
    let status = resp.status().as_u16();
    let text = resp
        .text()
        .map_err(|e| StreamlineError::Server(format!("read body: {e}")))?;
    let value: serde_json::Value = if text.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::String(text))
    };
    Ok((status, value))
}

pub fn delete(base: &str, path: &str) -> Result<()> {
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let resp = client()?
        .delete(&url)
        .send()
        .map_err(|e| StreamlineError::Server(format!("DELETE {url}: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        return Err(StreamlineError::Server(format!(
            "DELETE {url} -> HTTP {status}: {body}"
        )));
    }
    Ok(())
}

fn parse<T: DeserializeOwned>(resp: reqwest::blocking::Response) -> Result<T> {
    let status = resp.status();
    let url = resp.url().clone();
    let text = resp
        .text()
        .map_err(|e| StreamlineError::Server(format!("read body: {e}")))?;
    if !status.is_success() {
        return Err(StreamlineError::Server(format!(
            "{url} -> HTTP {status}: {}",
            text.chars().take(512).collect::<String>()
        )));
    }
    serde_json::from_str(&text).map_err(|e| {
        StreamlineError::Server(format!("decode JSON from {url}: {e}; body={text}"))
    })
}
