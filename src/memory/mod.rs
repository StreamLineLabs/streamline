//! Agent Memory Fabric (M1).
//!
//! Three memory tiers, each backed by a Streamline topic:
//!   * `episodic`  — append-only event log (raw observations)
//!   * `semantic`  — vector-indexed facts (uses M2 semantic topics)
//!   * `procedural` — learned procedures / how-tos (compact, key=skill)
//!
//! Routing is performed by `tier_router::route` based on the incoming
//! `MemoryWrite.kind`. MCP tools (`recall`, `remember`, `share`) are
//! registered in `src/mcp/tools.rs` and call into this module.
//!
//! Stability tier: **Experimental**. See ADR `0019-agent-memory-tiers.md`.

pub mod acl;
pub mod audit;
pub mod decay;
pub mod encryption;
pub mod export;
pub mod gdpr;
pub mod shared;
pub mod telemetry;
pub mod tenant;
pub mod tier_router;

/// Logical memory tier — maps to a topic naming convention
/// `__mem.<agent>.<tier>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Episodic,
    Semantic,
    Procedural,
}

impl Tier {
    pub fn topic_suffix(self) -> &'static str {
        match self {
            Tier::Episodic => "episodic",
            Tier::Semantic => "semantic",
            Tier::Procedural => "procedural",
        }
    }
}

/// A single memory write request.
#[derive(Debug, Clone)]
pub struct MemoryWrite {
    pub agent_id: String,
    pub kind: WriteKind,
    pub content: String,
    pub importance: f32,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum WriteKind {
    /// Raw observation — always episodic.
    Observation,
    /// A learned fact — semantic + episodic mirror.
    Fact,
    /// A procedure / skill — procedural (key = skill name).
    Procedure { skill: String },
}

/// Result of a `recall()` call.
#[derive(Debug, Clone)]
pub struct RecalledMemory {
    pub tier: Tier,
    pub topic: String,
    pub offset: i64,
    pub content: String,
    pub score: f32,
}
