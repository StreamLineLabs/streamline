//! Request/response types for the agent memory API.

use serde::{Deserialize, Serialize};

use crate::memory::{Tier, WriteKind};

/// Wire kind for a memory write. `procedure` requires a `skill` field.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase", tag = "kind")]
pub enum WriteKindWire {
    Observation,
    Fact,
    Procedure { skill: String },
}

impl From<WriteKindWire> for WriteKind {
    fn from(w: WriteKindWire) -> Self {
        match w {
            WriteKindWire::Observation => WriteKind::Observation,
            WriteKindWire::Fact => WriteKind::Fact,
            WriteKindWire::Procedure { skill } => WriteKind::Procedure { skill },
        }
    }
}

/// `POST /api/v1/memory/remember` request body.
#[derive(Debug, Deserialize)]
pub struct RememberRequest {
    pub agent_id: String,
    #[serde(flatten)]
    pub kind: WriteKindWire,
    pub content: String,
    #[serde(default = "default_importance")]
    pub importance: f32,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_importance() -> f32 {
    0.5
}

/// `POST /api/v1/memory/remember` response body.
#[derive(Debug, Serialize)]
pub struct RememberResponse {
    /// One entry per tier the write fanned out to.
    pub written: Vec<WrittenEntry>,
}

/// A single tier-write confirmation within a [`RememberResponse`].
#[derive(Debug, Serialize)]
pub struct WrittenEntry {
    pub topic: String,
    pub offset: i64,
}

/// `POST /api/v1/memory/recall` request body.
#[derive(Debug, Deserialize)]
pub struct RecallRequest {
    pub agent_id: String,
    pub query: String,
    #[serde(default = "default_k")]
    pub k: usize,
    /// Minimum semantic hits before falling back to episodic scan.
    #[serde(default)]
    pub min_hits: usize,
}

fn default_k() -> usize {
    10
}

/// `POST /api/v1/memory/recall` response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecallResponse {
    pub hits: Vec<RecalledHit>,
}

/// A single recall hit with its source tier, topic, and similarity score.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecalledHit {
    pub tier: String,
    pub topic: String,
    pub offset: i64,
    pub content: String,
    pub score: f32,
}

/// Per-tier memory counts in a [`StatsResponse`].
#[derive(Debug, Serialize, Deserialize)]
pub struct TierCounts {
    pub episodic: u64,
    pub semantic: u64,
    pub procedural: u64,
}

/// `GET /api/v1/memory/agents/:agent_id/stats` response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    pub agent_id: String,
    pub recall_total: u64,
    pub remember_total: u64,
    pub tiers: TierCounts,
}

/// `POST /api/v1/memory/agents/:agent_id/export` request body.
#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub since: Option<i64>,
    #[serde(default)]
    pub until: Option<i64>,
    #[serde(default)]
    pub tier: Option<String>,
}

/// `POST /api/v1/memory/agents/:agent_id/export` response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportResponse {
    pub records_exported: u64,
    pub bytes_written: u64,
    pub lines: Vec<String>,
}

/// `DELETE /api/v1/memory/agents/:agent_id` response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: bool,
    pub topics_purged: Vec<String>,
    pub records_purged: u64,
}

pub(crate) fn parse_tier(s: &str) -> Result<Tier, &'static str> {
    match s.to_ascii_lowercase().as_str() {
        "episodic" => Ok(Tier::Episodic),
        "semantic" => Ok(Tier::Semantic),
        "procedural" => Ok(Tier::Procedural),
        _ => Err("tier must be one of: episodic, semantic, procedural"),
    }
}

pub(crate) fn validate_remember(req: &RememberRequest) -> Result<(), &'static str> {
    if req.agent_id.trim().is_empty() {
        return Err("agent_id must not be empty");
    }
    if req.content.trim().is_empty() {
        return Err("content must not be empty");
    }
    if !(0.0..=1.0).contains(&req.importance) {
        return Err("importance must be in [0.0, 1.0]");
    }
    Ok(())
}
