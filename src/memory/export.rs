//! Memory export for compliance and audit (M1 P2).
//!
//! Exports agent memories in JSONL format for GDPR compliance.

use super::Tier;

/// Which serialization format to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Jsonl,
    Csv,
}

/// Parameters for an export job.
#[derive(Debug, Clone)]
pub struct ExportConfig {
    pub agent_id: String,
    /// Only include memories written at or after this unix epoch (seconds).
    pub since: Option<i64>,
    /// Only include memories written before this unix epoch (seconds).
    pub until: Option<i64>,
    /// Restrict to a single tier; `None` exports all tiers.
    pub tier: Option<Tier>,
    pub format: ExportFormat,
}

/// Summary statistics for a completed export.
#[derive(Debug, Clone, Default)]
pub struct ExportResult {
    pub records_exported: u64,
    pub bytes_written: u64,
}

/// Export agent memories as serialized lines.
///
/// Returns a `Vec<String>` of formatted rows (JSONL or CSV) plus aggregate
/// stats. This placeholder reads from the in-memory content store; a
/// production implementation would stream from the topic log.
pub fn export_memories(config: &ExportConfig) -> Result<(Vec<String>, ExportResult), ExportError> {
    use super::tier_router;

    let tiers = match config.tier {
        Some(t) => vec![t],
        None => vec![Tier::Episodic, Tier::Semantic, Tier::Procedural],
    };

    let mut lines = Vec::new();

    if config.format == ExportFormat::Csv {
        let header = "tier,topic,offset,content".to_string();
        lines.push(header);
    }

    for tier in &tiers {
        let topic = format!("__mem.{}.{}", config.agent_id, tier.topic_suffix());
        let entries = tier_router::content_store_entries(&topic);

        for (offset, content) in entries {
            let line = match config.format {
                ExportFormat::Jsonl => {
                    format!(
                        r#"{{"tier":"{}","topic":"{}","offset":{},"content":{}}}"#,
                        tier.topic_suffix(),
                        topic,
                        offset,
                        serde_json::to_string(&content).unwrap_or_default(),
                    )
                }
                ExportFormat::Csv => {
                    let escaped = content.replace('"', "\"\"");
                    format!(
                        "{},{},{},\"{}\"",
                        tier.topic_suffix(),
                        topic,
                        offset,
                        escaped,
                    )
                }
            };
            lines.push(line);
        }
    }

    let bytes_written = lines.iter().map(|l| l.len() as u64).sum();
    let records_exported = lines.len() as u64
        - if config.format == ExportFormat::Csv {
            1
        } else {
            0
        };

    Ok((
        lines,
        ExportResult {
            records_exported,
            bytes_written,
        },
    ))
}

/// Errors that can occur during memory export.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("io: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::tier_router;
    use crate::memory::{MemoryWrite, WriteKind};

    fn seed_memories() {
        let agent = format!("export-agent-{}", std::process::id());
        tier_router::reset_for_tests();
        let w = MemoryWrite {
            agent_id: agent.clone(),
            kind: WriteKind::Observation,
            content: "observation one".into(),
            importance: 0.5,
            tags: vec![],
        };
        tier_router::remember(&w).unwrap();

        let w2 = MemoryWrite {
            agent_id: agent.clone(),
            kind: WriteKind::Fact,
            content: "fact one".into(),
            importance: 0.8,
            tags: vec![],
        };
        tier_router::remember(&w2).unwrap();
    }

    #[test]
    fn jsonl_export_contains_records() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        seed_memories();
        let agent = format!("export-agent-{}", std::process::id());
        let cfg = ExportConfig {
            agent_id: agent,
            since: None,
            until: None,
            tier: None,
            format: ExportFormat::Jsonl,
        };
        let (lines, result) = export_memories(&cfg).unwrap();
        assert!(result.records_exported >= 2, "expected ≥ 2 records");
        assert!(result.bytes_written > 0);
        assert!(lines[0].contains("tier"));
    }

    #[test]
    fn csv_export_has_header() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        seed_memories();
        let agent = format!("export-agent-{}", std::process::id());
        let cfg = ExportConfig {
            agent_id: agent,
            since: None,
            until: None,
            tier: Some(Tier::Episodic),
            format: ExportFormat::Csv,
        };
        let (lines, _) = export_memories(&cfg).unwrap();
        assert_eq!(lines[0], "tier,topic,offset,content");
    }

    #[test]
    fn export_empty_agent() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        tier_router::reset_for_tests();
        let cfg = ExportConfig {
            agent_id: "nobody".into(),
            since: None,
            until: None,
            tier: None,
            format: ExportFormat::Jsonl,
        };
        let (lines, result) = export_memories(&cfg).unwrap();
        assert!(lines.is_empty());
        assert_eq!(result.records_exported, 0);
    }

    #[test]
    fn tier_filter_narrows_output() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        seed_memories();
        let cfg = ExportConfig {
            agent_id: "export-agent".into(),
            since: None,
            until: None,
            tier: Some(Tier::Procedural),
            format: ExportFormat::Jsonl,
        };
        let (lines, result) = export_memories(&cfg).unwrap();
        assert_eq!(result.records_exported, 0);
        assert!(lines.is_empty());
    }
}
