//! MCP tool definitions for agent memory (M1 P1).
//!
//! Three tools exposed to MCP clients (Claude Desktop, Cursor, etc.):
//!   * `recall(query, k=10)` — returns top-k memories.
//!   * `remember(content, kind, importance=0.5, tags=[])` — writes.
//!   * `share(memory_id, with_agent_id, ttl_secs)` — grants ACL.
//!
//! Stability tier: **Experimental**. Not yet registered in the MCP server's
//! tool registry — wire when M1 P1 lands.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RecallArgs {
    pub query: String,
    #[serde(default = "default_k")]
    pub k: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RememberArgs {
    pub content: String,
    /// "observation" | "fact" | "procedure"
    pub kind: String,
    #[serde(default)]
    pub skill: Option<String>,
    #[serde(default = "default_importance")]
    pub importance: f32,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShareArgs {
    pub memory_id: String,
    pub with_agent_id: String,
    #[serde(default = "default_ttl")]
    pub ttl_secs: u64,
}

fn default_k() -> usize { 10 }
fn default_importance() -> f32 { 0.5 }
fn default_ttl() -> u64 { 86_400 }

/// Tool descriptors as the MCP server expects.
pub fn tool_descriptors() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "recall",
            "Search the agent's long-term memory for the top-k most relevant entries.",
        ),
        (
            "remember",
            "Persist a new memory: observation | fact | procedure.",
        ),
        (
            "share",
            "Grant another agent time-bounded read access to a memory.",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_args_default_k() {
        let a: RecallArgs = serde_json::from_str(r#"{"query":"x"}"#).unwrap();
        assert_eq!(a.k, 10);
    }

    #[test]
    fn three_tools_registered() {
        assert_eq!(tool_descriptors().len(), 3);
    }
}
